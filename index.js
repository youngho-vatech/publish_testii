const dynamo = ({
  api,
  checkStatusToDynamoDbName,
  createModels,
  getSecretString,
  restoreTableFromBackup
} = require("./dynamoDB/api/dynamoDB.js"));
const test = params => {
  console.log("asdf", params);
  console.log("뭐해");
  return params;
};
const testtest = params => {
  console.log("asdf2", params);
  console.log("뭐해2");
  return params;
};
export { test, testtest };
