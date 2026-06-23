import logging
import os
from typing import Any

from google import genai
from google.genai import types

from .context import get_credentials
from .vertex_embedder import VertexAIEmbedder

logger = logging.getLogger(__name__)


class DocumentRAGEngine:
    def __init__(
        self,
        project_id: str,
        location: str = "us-central1",
        credentials: Any | None = None,
    ):
        self.project_id = project_id
        self.location = location
        self._credentials = credentials or get_credentials(project_id)
        self.embedder = VertexAIEmbedder(
            project_id, location, credentials=self._credentials
        )
        self.chunks = []
        self.embeddings = []
        self._client = None

    def _get_client(self):
        if self._client is None:
            try:
                self._client = genai.Client(
                    project=self.project_id,
                    location=self.location,
                    credentials=self._credentials,
                    vertexai=True,
                )
            except Exception as e:
                logger.error(f"Failed to initialize GenAI Client: {e}")
        return self._client

    def load_document(
        self, file_path: str, chunk_size: int = 1000, chunk_overlap: int = 200
    ):
        """Loads a document (PDF or TXT), extracts text, chunks it, and generates embeddings."""
        logger.info(f"Loading document from {file_path}")

        text = ""
        ext = os.path.splitext(file_path)[1].lower()

        if ext in [".txt", ".pdf", ".md"]:
            text = self._extract_text_via_gemini(file_path)
        elif ext == ".docx":
            raise ValueError(
                "DOCX files are not supported directly by Gemini in this setup. Please convert to PDF or TXT first."
            )
        else:
            raise ValueError(f"Unsupported file extension: {ext}")

        if not text:
            logger.error("No text extracted from document.")
            return

        # 2. Chunk text
        new_chunks = self._chunk_text(text, chunk_size, chunk_overlap)
        self.chunks.extend(new_chunks)
        logger.info(f"Created {len(new_chunks)} chunks from document.")

        # Move to debug logs
        logger.debug(f"Created {len(new_chunks)} chunks")
        for i, chunk in enumerate(new_chunks):
            logger.debug(f"--- Chunk {i + 1} ---")
            logger.debug(chunk)
            logger.debug("-" * 20)

        # 3. Generate embeddings
        if new_chunks:
            logger.info("Generating embeddings for chunks...")
            new_embeddings = self.embedder.get_embeddings(
                new_chunks, task_type="RETRIEVAL_DOCUMENT"
            )
            self.embeddings.extend(new_embeddings)
            logger.info("Embeddings generated successfully.")

    def _extract_text_via_gemini(self, file_path: str) -> str:
        """Uses Gemini to extract text and format as Markdown, with hash-based caching."""
        import hashlib

        try:
            with open(file_path, "rb") as f:
                file_bytes = f.read()
        except Exception as e:
            logger.error(f"Failed to read file: {e}")
            return ""

        # Calculate hash
        file_hash = hashlib.md5(file_bytes).hexdigest()

        # Cache directory in project root (.rag_cache)
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(current_dir, "../.."))
        cache_dir = os.path.join(project_root, ".rag_cache")
        os.makedirs(cache_dir, exist_ok=True)

        base_name = os.path.basename(file_path)
        cached_path = os.path.join(cache_dir, f"{base_name}.{file_hash}.md")

        if os.path.exists(cached_path):
            logger.info(f"Found cached Markdown file: {cached_path}")
            with open(cached_path, encoding="utf-8") as f:
                return f.read()

        logger.info(
            f"Extracting structured governance data via Gemini: {file_path}"
        )
        client = self._get_client()
        if not client:
            return ""

        try:
            # Determine mime type
            ext = os.path.splitext(file_path)[1].lower()
            if ext == ".pdf":
                mime_type = "application/pdf"
            elif ext in [".txt", ".md"]:
                mime_type = "text/plain"
            else:
                mime_type = "application/octet-stream"

            import time

            start_time = time.time()

            response = client.models.generate_content(
                model="gemini-2.5-pro",  # Use pro for better extraction
                contents=[
                    types.Part.from_bytes(data=file_bytes, mime_type=mime_type),
                    "Analyze this document and extract ONLY the information relevant to data governance, "
                    "table definitions, column descriptions, business glossary terms, and data classification/policy tags. "
                    "Ignore all other information such as infrastructure setup, deployment steps, project background, etc. "
                    "Format the extracted information as clean Markdown using the following rules:\n"
                    "1. Use `# Table: [Name]` as the header for a table section.\n"
                    "2. Use `# Business Glossary for Table: [Name]` for glossary terms belonging to that table.\n"
                    "3. Use `# Policy Tags for Table: [Name]` for classification rules belonging to that table.\n"
                    "Use bold text for sub-sections (e.g., **Column Definitions**). "
                    "Do not summarize the content, just extract the relevant parts accurately.",
                ],
                config=types.GenerateContentConfig(temperature=0.0),
            )

            duration = time.time() - start_time
            logger.info(
                f"=== DEBUG: Gemini Extraction took {duration:.2f} seconds ==="
            )

            if hasattr(response, "usage_metadata") and response.usage_metadata:
                logger.info(
                    f"Tokens consumed: {response.usage_metadata.total_token_count}"
                )

            # Save to cache
            try:
                with open(cached_path, "w", encoding="utf-8") as f:
                    f.write(response.text)
                logger.info(f"Saved cached Markdown file: {cached_path}")
            except Exception as e:
                logger.warning(f"Failed to save cache file: {e}")

            return response.text
        except Exception as e:
            logger.error(f"Failed to extract text via Gemini: {e}")
            return ""

    def _chunk_text(
        self, text: str, chunk_size: int, chunk_overlap: int
    ) -> list[str]:
        """Chunks text by Markdown headers (# or ##)."""
        import re

        # Split by lines starting with # or ##
        # We use positive lookahead to keep the delimiter in the split result
        chunks = re.split(r"(?m)^(?=#+\s)", text)

        # Filter out empty chunks
        chunks = [c.strip() for c in chunks if c.strip()]

        # Fallback if a chunk is still too large
        final_chunks = []
        for chunk in chunks:
            if len(chunk) > chunk_size * 2:
                # Line-aware fallback to avoid breaking words/sentences
                lines = chunk.split("\n")

                # Extract header if exists to preserve context across splits
                header_line = ""
                if lines and lines[0].startswith("#"):
                    header_line = lines[0]
                    lines = lines[1:]  # Process remaining lines

                current_chunk = []
                current_length = 0

                for line in lines:
                    if (
                        current_length + len(line) > chunk_size
                        and current_chunk
                    ):
                        chunk_content = "\n".join(current_chunk)
                        if header_line:
                            chunk_content = header_line + "\n" + chunk_content
                        final_chunks.append(chunk_content)

                        # Handle overlap by keeping lines from the end that fit in chunk_overlap
                        overlap_chunk = []
                        overlap_len = 0
                        for l in reversed(current_chunk):
                            if overlap_len + len(l) < chunk_overlap:
                                overlap_chunk.insert(0, l)
                                overlap_len += len(l) + 1
                            else:
                                break
                        current_chunk = [*overlap_chunk, line]
                        current_length = sum(len(l) + 1 for l in current_chunk)
                    else:
                        current_chunk.append(line)
                        current_length += len(line) + 1

                if current_chunk:
                    chunk_content = "\n".join(current_chunk)
                    if header_line:
                        chunk_content = header_line + "\n" + chunk_content
                    final_chunks.append(chunk_content)
            else:
                final_chunks.append(chunk)

        return final_chunks

    def retrieve(self, query: str, top_k: int = 3) -> list[dict[str, Any]]:
        """Retrieves top_k most relevant chunks for a query."""
        if not self.chunks or not self.embeddings:
            logger.warning("No document loaded or embeddings missing.")
            return []

        query_emb = self.embedder.get_embedding(
            query, task_type="RETRIEVAL_QUERY"
        )
        if not query_emb:
            return []

        similarities = []
        for i, chunk_emb in enumerate(self.embeddings):
            sim = self.embedder.cosine_similarity(query_emb, chunk_emb)
            similarities.append((i, sim))

        # Sort by similarity
        similarities.sort(key=lambda x: x[1], reverse=True)

        results = []
        for i in range(min(top_k, len(similarities))):
            idx, sim = similarities[i]
            results.append({"text": self.chunks[idx], "score": sim})

        # Move to debug logs
        logger.debug(f"Retrieved {len(results)} chunks for query: '{query}'")
        for i, res in enumerate(results):
            logger.debug(f"Rank {i + 1} (Score: {res['score']:.4f}):")
            logger.debug(res["text"])
            logger.debug("-" * 20)

        return results
