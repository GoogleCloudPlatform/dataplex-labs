"""GlossaryId packs information about the project, entry group and glossary."""

import dataclasses


@dataclasses.dataclass(frozen=True)
class GlossaryId:
  project_id: str
  location: str
  entry_group: str
  glossary_id: str
