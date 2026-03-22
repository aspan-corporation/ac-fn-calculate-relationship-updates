#!/usr/bin/env node
import * as cdk from "aws-cdk-lib/core";
import { AcFnCalculateRelationshipUpdatesStack } from "../lib/ac-fn-calculate-relationship-updates-stack.ts";

const app = new cdk.App();
new AcFnCalculateRelationshipUpdatesStack(
  app,
  "AcFnCalculateRelationshipUpdatesStack",
  {
    env: {
      account: process.env.CDK_DEFAULT_ACCOUNT,
      region: process.env.CDK_DEFAULT_REGION,
    },
  },
);
