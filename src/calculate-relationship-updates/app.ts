import { withMiddlewares } from "@aspan-corporation/ac-shared";
import { lambdaHandler } from "./eventHandler.js";

export const handler = withMiddlewares(lambdaHandler);
