# Knowledge Catalog Discovery Agent

## About

The Knowledge Catalog Discovery Agent is an AI-powered search assistant for discovering data assets in Google Cloud. While standard semantic search only matches semantically similar text, this agent goes further by performing semantic decomposition of complex questions, generating multiple relevant search queries, and reranking the final results to provide a comprehensive answer.

## 🚀 Quick Start

### 1. Prerequisites

You need a Google Cloud project with the following APIs enabled:
* Knowledge Catalog (`dataplex.googleapis.com`)
* Vertex AI (`aiplatform.googleapis.com`)
* Service Usage API (`serviceusage.googleapis.com`)

Ensure you have the following permissions via IAM Roles:
* `dataplex.projects.search` - can be obtained via roles like `roles/dataplex.viewer`.
* `aiplatform.endpoints.predict` - can be obtained via roles like `roles/aiplatform.user`.
* `serviceusage.services.use` - needed to use the current project as the quota project. Can be obtained via roles like `roles/serviceusage.serviceUsageConsumer`.

### 2. Installation

Clone the Github Repository:  
```shell  
git clone https://github.com/GoogleCloudPlatform/dataplex-labs.git  
```

Install the dependencies. It is recommended to create a Python Virtual Environment:  
```shell  
python3 -m venv /tmp/kcsearch  
source /tmp/kcsearch/bin/activate  
pip3 install -r requirements.txt  
```

### 3. Configuration

Set the required environment variables:  
```shell

# Replace <PROJECT_ID> with your consumer project ID.
export GOOGLE_CLOUD_PROJECT=<PROJECT_ID>
export GOOGLE_GENAI_USE_VERTEXAI=True

```

### 4. Run the Agent

Run the agent using the ADK CLI:  
```shell
adk run path/to/agent/parent/folder
```
Replace `path/to/agent/parent/folder` with the relative or absolute path to the **parent** directory of the one containing the agent's source code.

## Suggested Directory Structure

```
my_custom_agent/
├── agent.py
└── knowledge_catalog_discovery_agent/
    ├── SKILL.md
    ├── agent.py
    ├── tools.py
    └── utils.py
```
Replace `my_custom_agent` with the name of the parent agent that is leveraging the Knowledge Catalog Search Agent as a sub-agent.

## References

* [Knowledge Catalog Search API](http://docs.cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations/searchEntries)
* [ADK Documentation](https://adk.dev/get-started/python/#run-your-agent)
* [Predefined IAM Roles](https://docs.cloud.google.com/iam/docs/roles-permissions)

