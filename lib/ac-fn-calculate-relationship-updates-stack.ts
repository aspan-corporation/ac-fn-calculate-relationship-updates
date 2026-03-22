import * as cdk from "aws-cdk-lib";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as lambdaNodejs from "aws-cdk-lib/aws-lambda-nodejs";
import * as logs from "aws-cdk-lib/aws-logs";
import * as pipes from "aws-cdk-lib/aws-pipes";
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as ssm from "aws-cdk-lib/aws-ssm";
import { Construct } from "constructs";
import { fileURLToPath } from "node:url";
import * as path from "path";

const currentFilePath = fileURLToPath(import.meta.url);
const currentDirPath = path.dirname(currentFilePath);

export class AcFnCalculateRelationshipUpdatesStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Get centralized log group from monitoring stack
    const centralLogGroupArn = ssm.StringParameter.valueForStringParameter(
      this,
      "/ac/monitoring/central-log-group-arn",
    );
    const centralLogGroup = logs.LogGroup.fromLogGroupArn(
      this,
      "CentralLogGroup",
      centralLogGroupArn,
    );

    // SFN logging requires ARN without the CDK-appended :* suffix
    const centralLogGroupName = ssm.StringParameter.valueForStringParameter(
      this,
      "/ac/monitoring/central-log-group-name",
    );
    const sfnLogGroupArn = `arn:aws:logs:${this.region}:${this.account}:log-group:${centralLogGroupName}`;

    // Get table names from SSM
    const searchTableName = ssm.StringParameter.valueForStringParameter(
      this,
      "/ac/data/search-table-name",
    );
    const tagsTableName = ssm.StringParameter.valueForStringParameter(
      this,
      "/ac/data/tags-table-name",
    );
    const metaTableStreamArn = ssm.StringParameter.valueForStringParameter(
      this,
      "/ac/data/meta-table-stream-arn",
    );

    // Create the Lambda function
    const calculateRelationshipUpdatesFunction =
      new lambdaNodejs.NodejsFunction(
        this,
        "CalculateRelationshipUpdatesProcessor",
        {
          functionName: "CalculateRelationshipUpdatesProcessor",
          entry: path.join(
            currentDirPath,
            "../src/calculate-relationship-updates/app.ts",
          ),
          handler: "handler",
          runtime: lambda.Runtime.NODEJS_22_X,
          memorySize: 128,
          timeout: cdk.Duration.seconds(60),
          logGroup: centralLogGroup,
          environment: {
            LOG_LEVEL: "INFO",
            POWERTOOLS_SERVICE_NAME: "ac-fn-calculate-relationship-updates",
          },
        },
      );


    // Create the Step Functions state machine
    const searchTableArn = cdk.Arn.format(
      {
        partition: "aws",
        service: "dynamodb",
        region: this.region,
        account: this.account,
        resource: `table/${searchTableName}`,
      },
      this,
    );
    const tagsTableArn = cdk.Arn.format(
      {
        partition: "aws",
        service: "dynamodb",
        region: this.region,
        account: this.account,
        resource: `table/${tagsTableName}`,
      },
      this,
    );

    const stateMachine = new sfn.CfnStateMachine(
      this,
      "ProcessMetadataUpdatesStateMachine",
      {
        stateMachineType: "EXPRESS",
        definitionString: JSON.stringify({
          Comment: "ProcessMetadataUpdatesStepFunction",
          StartAt: "extract metadata",
          States: {
            "extract metadata": {
              Type: "Task",
              Resource: "arn:aws:states:::lambda:invoke",
              OutputPath: "$.Payload",
              TimeoutSeconds: 60,
              Parameters: {
                "Payload.$": "$",
                FunctionName:
                  "${CalculateRelationshipUpdatesFunctionArn}",
              },
              Retry: [
                {
                  ErrorEquals: [
                    "Lambda.ServiceException",
                    "Lambda.AWSLambdaException",
                    "Lambda.SdkClientException",
                    "Lambda.TooManyRequestsException",
                  ],
                  IntervalSeconds: 2,
                  MaxAttempts: 10,
                  BackoffRate: 2,
                  JitterStrategy: "FULL",
                },
              ],
              Next: "process user tags",
            },
            "process user tags": {
              Type: "Map",
              ItemProcessor: {
                ProcessorConfig: {
                  Mode: "INLINE",
                },
                StartAt: "check operation type",
                States: {
                  "check operation type": {
                    Type: "Choice",
                    Choices: [
                      {
                        Variable: "$.type",
                        StringEquals: "add",
                        Next: "Parallel",
                      },
                      {
                        Variable: "$.type",
                        StringEquals: "delete",
                        Next: "delete search",
                      },
                    ],
                    Default: "No such operation for user tag",
                  },
                  "delete search": {
                    Type: "Task",
                    Resource: "arn:aws:states:::dynamodb:deleteItem",
                    Parameters: {
                      "TableName": "${SearchTableName}",
                      Key: {
                        "key.$": "$.key.S",
                        "id.$": "$.id.S",
                      },
                    },
                    Next: "query search",
                    ResultPath: null,
                  },
                  "query search": {
                    Type: "Task",
                    Parameters: {
                      "TableName": "${SearchTableName}",
                      KeyConditionExpression: "#key = :key",
                      ExpressionAttributeValues: {
                        ":key": {
                          "S.$": "$.key.S",
                        },
                      },
                      ExpressionAttributeNames: {
                        "#key": "key",
                      },
                      Limit: 2,
                      Select: "COUNT",
                    },
                    Resource:
                      "arn:aws:states:::aws-sdk:dynamodb:query",
                    ResultPath: "$.result",
                    ResultSelector: {
                      "count.$": "$.Count",
                    },
                    Next: "check if no more searches exist",
                  },
                  "No such operation for user tag": {
                    Type: "Fail",
                  },
                  Parallel: {
                    Type: "Parallel",
                    End: true,
                    Branches: [
                      {
                        StartAt: "add user tag to search",
                        States: {
                          "add user tag to search": {
                            Type: "Task",
                            Resource:
                              "arn:aws:states:::dynamodb:putItem",
                            Parameters: {
                              "TableName": "${SearchTableName}",
                              Item: {
                                "id.$": "$.id.S",
                                "key.$": "$.key.S",
                                "value.$": "$.value.S",
                              },
                            },
                            End: true,
                          },
                        },
                      },
                      {
                        StartAt: "add tag",
                        States: {
                          "add tag": {
                            Type: "Task",
                            Resource:
                              "arn:aws:states:::dynamodb:putItem",
                            Parameters: {
                              "TableName": "${TagsTableName}",
                              Item: {
                                "key#value.$": "$.key#value",
                              },
                            },
                            End: true,
                          },
                        },
                      },
                    ],
                  },
                  "check if no more searches exist": {
                    Type: "Choice",
                    Choices: [
                      {
                        Variable: "$.result.count",
                        NumericEquals: 0,
                        Next: "delete tag",
                      },
                    ],
                    Default: "Pass",
                  },
                  "delete tag": {
                    Type: "Task",
                    Resource: "arn:aws:states:::dynamodb:deleteItem",
                    Parameters: {
                      "TableName": "${TagsTableName}",
                      Key: {
                        "key#value": {
                          "S.$": "$.key#value.S",
                        },
                      },
                    },
                    Next: "Pass",
                  },
                  Pass: {
                    Type: "Pass",
                    End: true,
                  },
                },
              },
              End: true,
            },
          },
        }),
        definitionSubstitutions: {
          CalculateRelationshipUpdatesFunctionArn:
            calculateRelationshipUpdatesFunction.functionArn,
          SearchTableName: searchTableName,
          TagsTableName: tagsTableName,
        },
        loggingConfiguration: {
          destinations: [
            {
              cloudWatchLogsLogGroup: {
                logGroupArn: sfnLogGroupArn,
              },
            },
          ],
          includeExecutionData: true,
          level: "ALL",
        },
        tracingConfiguration: {
          enabled: true,
        },
        roleArn: new iam.Role(this, "StateMachineRole", {
          assumedBy: new iam.ServicePrincipal("states.amazonaws.com"),
          inlinePolicies: {
            LambdaInvoke: new iam.PolicyDocument({
              statements: [
                new iam.PolicyStatement({
                  actions: ["lambda:InvokeFunction"],
                  resources: [
                    calculateRelationshipUpdatesFunction.functionArn,
                  ],
                }),
              ],
            }),
            DynamoAccess: new iam.PolicyDocument({
              statements: [
                new iam.PolicyStatement({
                  actions: [
                    "dynamodb:PutItem",
                    "dynamodb:DeleteItem",
                    "dynamodb:Query",
                  ],
                  resources: [searchTableArn, tagsTableArn],
                }),
              ],
            }),
            CloudWatchLogs: new iam.PolicyDocument({
              statements: [
                new iam.PolicyStatement({
                  actions: [
                    "logs:CreateLogDelivery",
                    "logs:GetLogDelivery",
                    "logs:UpdateLogDelivery",
                    "logs:DeleteLogDelivery",
                    "logs:ListLogDeliveries",
                    "logs:PutResourcePolicy",
                    "logs:DescribeResourcePolicies",
                    "logs:DescribeLogGroups",
                  ],
                  resources: ["*"],
                }),
              ],
            }),
            XRay: new iam.PolicyDocument({
              statements: [
                new iam.PolicyStatement({
                  actions: [
                    "xray:PutTraceSegments",
                    "xray:PutTelemetryRecords",
                    "xray:GetSamplingRules",
                    "xray:GetSamplingTargets",
                  ],
                  resources: ["*"],
                }),
              ],
            }),
          },
        }).roleArn,
      },
    );

    // Create Pipe DLQ
    const pipeDlq = new sqs.Queue(this, "PipeDLQ", {
      retentionPeriod: cdk.Duration.days(14),
    });

    // Create Pipe execution role
    const pipeExecutionRole = new iam.Role(this, "PipeExecutionRole", {
      assumedBy: new iam.ServicePrincipal("pipes.amazonaws.com"),
      inlinePolicies: {
        DynamoAccess: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              actions: [
                "dynamodb:DescribeStream",
                "dynamodb:GetRecords",
                "dynamodb:GetShardIterator",
                "dynamodb:ListStreams",
              ],
              resources: [metaTableStreamArn],
            }),
          ],
        }),
        SqsAccess: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              actions: [
                "sqs:SendMessage",
                "sqs:GetQueueAttributes",
                "sqs:GetQueueUrl",
              ],
              resources: [pipeDlq.queueArn],
            }),
          ],
        }),
        StepFnAccess: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              actions: [
                "states:StartExecution",
                "states:StartSyncExecution",
              ],
              resources: [stateMachine.attrArn],
            }),
          ],
        }),
      },
    });

    // Create EventBridge Pipe: DynamoDB stream → Step Functions
    new pipes.CfnPipe(this, "ProcessMetadataUpdatesPipe", {
      roleArn: pipeExecutionRole.roleArn,
      source: metaTableStreamArn,
      target: stateMachine.attrArn,
      sourceParameters: {
        dynamoDbStreamParameters: {
          startingPosition: "LATEST",
          deadLetterConfig: {
            arn: pipeDlq.queueArn,
          },
          maximumRetryAttempts: 5,
          batchSize: 10,
        },
      },
    });
  }
}
