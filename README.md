# ac-fn-calculate-relationship-updates

AWS Lambda function (CDK) that processes DynamoDB stream events from the metadata table via an EventBridge Pipe and Express Step Functions state machine. Calculates and applies relationship updates to the Search and Tags DynamoDB tables whenever metadata records are created, updated, or deleted.

## Overview

- **Trigger**: EventBridge Pipe sourced from DynamoDB stream on the metadata table
- **Orchestration**: Express Step Functions state machine routes `add`/`delete` operations per tag
- **Output**: Writes derived relationship data to the Search and Tags tables
- **Infrastructure**: Managed via AWS CDK

## SSM Inputs

| Parameter | Description |
|---|---|
| `/ac/monitoring/central-log-group-arn` | CloudWatch log group ARN for Lambda and Step Functions logging |
| `/ac/data/meta-table-stream-arn` | DynamoDB stream ARN for the metadata table (EventBridge Pipe source) |
| `/ac/data/search-table-name` | Search table name |
| `/ac/data/tags-table-name` | Tags table name |

## Development

```bash
npm ci
npm run build
npm test
```

## Deployment

Deployed automatically via GitHub Actions on push to `master` using OIDC authentication.
