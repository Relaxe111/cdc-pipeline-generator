# Author Identity

- **Source of truth:** `AUTHOR` env variable in `_tools/asma-cli/.env`
- **Usage:** Set `**Author:** $AUTHOR` in all ASMA documentation metadata headers.
- **Fallback:** When the `AUTHOR` env var is unavailable, read it from `_tools/asma-cli/.env` directly.
- **Document convention:** See `_tools/cdc_cli/.github/skills/document-organization/SKILL.md` section 3 (Metadata Field Rules).

## Do NOT

- Hardcode any specific author name in git-tracked files — always refer to the `AUTHOR` env var
- Use "CDC CLI Expert (AI)" or any AI persona name as the Author field in documents
- Omit the Author field on new proposals, plans, or architecture docs
