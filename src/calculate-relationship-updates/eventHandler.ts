import { unmarshall } from "@aws-sdk/util-dynamodb";
import { Context, DynamoDBRecord } from "aws-lambda";
import { AttributeValue } from "@aws-sdk/client-dynamodb";
import assert from "node:assert";
import type { TagInput } from "./graphqlTypes.js";
import { Logger } from "@aws-lambda-powertools/logger";

enum EventName {
  INSERT = "INSERT",
  REMOVE = "REMOVE",
  MODIFY = "MODIFY",
}

type DDBRecord = AttributeValue | Record<string, AttributeValue>;

type Image = {
  id: string;
  tags: TagInput[];
};

type MetaAtom = {
  type: "add" | "delete";
  id: { S: string };
  key: { S: string };
  value: { S: string };
  "key#value": { S: string };
};

type PreparedReturn = MetaAtom[];

const logger = new Logger();

export const lambdaHandler = async (
  dynamodbRecords: DynamoDBRecord[],
  { logger }: Context,
): Promise<PreparedReturn> => {
  const preparedReturn: PreparedReturn = extractMeta(dynamodbRecords, logger);
  logger.debug("metadata atoms", { metaAtoms: preparedReturn });

  return preparedReturn;
};

const NON_SEARCHABLE_TAGS = [
  "ac:tau:dateCreated",
  "ac:tau:latitude",
  "ac:tau:longitude",
  "ac:tau:size",
];

export const extractMeta = (
  records: DynamoDBRecord[],
  logger: Logger,
): PreparedReturn => {
  const mappedRecords = records
    .map(({ eventName, dynamodb }) => {
      assert(eventName, "DynamoDBRecord: eventName cannot be empty");
      assert(dynamodb, "DynamoDBRecord: dynamodb cannot be empty");

      const { OldImage, NewImage } = dynamodb;

      return {
        eventName,
        oldImage: OldImage
          ? unmarshall(OldImage as Record<string, AttributeValue>)
          : undefined,
        newImage: NewImage
          ? unmarshall(NewImage as Record<string, AttributeValue>)
          : undefined,
      };
    })
    .reduce((acc: MetaAtom[], { eventName, oldImage, newImage }) => {
      switch (eventName) {
        case EventName.INSERT:
          assert(newImage, "DynamoDBRecord: newImage cannot be empty");
          return [
            ...acc,
            ...newImage.tags.map((tag: TagInput) => ({
              type: "add" as MetaAtom["type"],
              ...idAndTagToDynamoDBJson(newImage.id, tag),
            })),
          ];
        case EventName.REMOVE:
          assert(oldImage, "DynamoDBRecord: oldImage cannot be empty");
          return [
            ...acc,
            ...oldImage.tags.map((tag: TagInput) => ({
              type: "delete" as MetaAtom["type"],
              ...idAndTagToDynamoDBJson(oldImage.id, tag),
            })),
          ];
        case EventName.MODIFY:
          assert(
            oldImage && newImage,
            "DynamoDBRecord: oldImage and newImage cannot be empty",
          );

          return [
            ...acc,
            ...calculateAddedTags(oldImage.tags, newImage.tags).map((tag) => ({
              type: "add" as MetaAtom["type"],
              ...idAndTagToDynamoDBJson(newImage.id, tag),
            })),
            ...calculateDeletedTags(oldImage.tags, newImage.tags).map(
              (tag) => ({
                type: "delete" as MetaAtom["type"],
                ...idAndTagToDynamoDBJson(newImage.id, tag),
              }),
            ),
          ];
        default:
          assert(false, "DynamoDBRecord: unrecognized eventName");
      }
    }, []);
  return mappedRecords.filter(
    ({ key }: MetaAtom) => !NON_SEARCHABLE_TAGS.includes(key.S),
  );
};

const idAndTagToDynamoDBJson = (id: string, tag: TagInput) => ({
  id: { S: id },
  key: { S: tag.key },
  value: { S: tag.value },
  "key#value": { S: `${tag.key}#${tag.value}` },
});

const calculateDeletedTags = (
  oldTags: TagInput[],
  newTags: TagInput[],
): TagInput[] =>
  oldTags.filter(
    ({ key: oldKey }) => !newTags?.find(({ key: newKey }) => oldKey === newKey),
  );

const calculateAddedTags = (
  oldTags: TagInput[],
  newTags: TagInput[],
): TagInput[] =>
  newTags.filter(
    ({ key: newKey }) => !oldTags?.find(({ key: oldKey }) => oldKey === newKey),
  );
