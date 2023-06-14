const mongoose = require("mongoose");
const mysql = require("mysql2/promise");
require("dotenv").config();

let connection;

async function connectDb() {
  try {
    await mongoose.connect(process.env.MONGO_ADDRESS + process.env.MONGO_DB, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
    });
    console.log("Mongo Connected");
  } catch (err) {
    console.log("Coulnt connect mongodb");
  }
}

async function connectMySQL() {
  try {
    connection = await mysql.createConnection({
      host: process.env.MYSQL_ADDRESS,
      user: process.env.MYSQL_USER,
      password: process.env.MYSQL_PASSWORD,
      database: process.env.MYSQL_DB,
    });
    console.log("My sql connected");
    await runSqlCommands();
  } catch (e) {
    console.log("Couldnt connect sql", e);
  }
}
async function executeQuery(query, params = null) {
  if (params) params = params.map((p) => p ?? null);
  try {
    // execute a query using async/await
    const [result] = await connection.execute(query, params && params);
    return result;
  } catch (error) {
    console.error("error", error);
  }
}

function closeDb() {
  connection.end();
  mongoose.disconnect();
}

const table = (name) => `CREATE TABLE IF NOT EXISTS ${name} (    
  _id LONGTEXT,
  categories LONGTEXT,
  tags LONGTEXT,
  document_links LONGTEXT,
  datasetfields LONGTEXT,
  authors LONGTEXT,
  emailNotifications LONGTEXT,
  showOrganisation LONGTEXT,
  structuralMetadata LONGTEXT,
  datasetVersionIsV1 LONGTEXT,
  isCohortDiscovery LONGTEXT,
  toolids LONGTEXT,
  datasetids LONGTEXT,
  timestamps LONGTEXT,
  relatedObjects LONGTEXT,
  programmingLanguage LONGTEXT,
  pid LONGTEXT,
  datasetVersion LONGTEXT,
  id LONGTEXT,
  datasetid LONGTEXT,
  name LONGTEXT,
  datasetv2 LONGTEXT,
  type LONGTEXT,
  activeflag LONGTEXT,
  source LONGTEXT,
  is5Safes LONGTEXT,
  questionAnswers LONGTEXT,
  createdAt LONGTEXT,
  updatedAt LONGTEXT,
  __v LONGTEXT,
  percentageCompleted LONGTEXT,
  applicationStatusDesc LONGTEXT,
  commercialUse LONGTEXT,
  counter LONGTEXT,
  description LONGTEXT,
  discourseTopicId LONGTEXT,
  hasTechnicalDetails LONGTEXT,
  applicationStatusAuthor LONGTEXT
  );`;

async function createTables(types) {
  console.log("Creating tables if not exists", types);
  for (let i = 0; i < types.length; i++) {
    const result = await connection.execute(table(types[i]));
  }
  console.log("Tables creation completed");
}

async function readData() {
  const MyModel = mongoose.model(
    process.env.MONGO_COLLECTION,
    new mongoose.Schema({}, { strict: false }),
    process.env.MONGO_COLLECTION
  );

  const distinctTypes = await MyModel.distinct("type");
  await createTables(distinctTypes);

  for (let typ = 0; typ < distinctTypes.length; typ++) {
    const documents = await MyModel.find({ type: distinctTypes[typ] });
    console.log(
      "Type ",
      distinctTypes[typ],
      " contains ",
      documents.length,
      " documents"
    );
    console.log("Inserting documents ...");
    //  Loop to insert data into table
    for (let i = 0; i < documents.length; i++) {
      const doc = documents[i];

      const {
        _id,
        categories,
        tags,
        document_links,
        datasetfields,
        authors,
        emailNotifications,
        showOrganisation,
        structuralMetadata,
        datasetVersionIsV1,
        isCohortDiscovery,
        toolids,
        datasetids,
        timestamps,
        relatedObjects,
        programmingLanguage,
        pid,
        datasetVersion,
        id,
        datasetid,
        name,
        datasetv2,
        type,
        activeflag,
        source,
        is5Safes,
        questionAnswers,
        createdAt,
        updatedAt,
        __v,
        percentageCompleted,
        applicationStatusDesc,
        commercialUse,
        counter,
        description,
        discourseTopicId,
        hasTechnicalDetails,
        applicationStatusAuthor,
      } = doc;

      const result = await executeQuery(
        `INSERT INTO ${distinctTypes[typ]} ( _id,
        categories,
        tags,
        document_links,
        datasetfields,
        authors,
        emailNotifications,
        showOrganisation,
        structuralMetadata,
        datasetVersionIsV1,
        isCohortDiscovery,
        toolids,
        datasetids,
        timestamps,
        relatedObjects,
        programmingLanguage,
        pid,
        datasetVersion,
        id,
        datasetid,
        name,
        datasetv2,
        type,
        activeflag,
        source,
        is5Safes,
        questionAnswers,
        createdAt,
        updatedAt,
        __v,
        percentageCompleted,
        applicationStatusDesc,
        commercialUse,
        counter,
        description,
        discourseTopicId,
        hasTechnicalDetails,
        applicationStatusAuthor) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
        [
          _id,
          categories,
          tags,
          document_links,
          datasetfields,
          authors,
          emailNotifications,
          showOrganisation,
          structuralMetadata,
          datasetVersionIsV1,
          isCohortDiscovery,
          toolids,
          datasetids,
          timestamps,
          relatedObjects,
          programmingLanguage,
          pid,
          datasetVersion,
          id,
          datasetid,
          name,
          datasetv2,
          type,
          activeflag,
          source,
          is5Safes,
          questionAnswers,
          createdAt,
          updatedAt,
          __v,
          percentageCompleted,
          applicationStatusDesc,
          commercialUse,
          counter,
          description,
          discourseTopicId,
          hasTechnicalDetails,
          applicationStatusAuthor,
        ]
      );
    }
  }

  await closeDb();
}

connectDb();
connectMySQL();

async function runSqlCommands() {
  await readData();
}
