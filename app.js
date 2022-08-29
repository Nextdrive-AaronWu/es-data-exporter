const axios = require("axios");
const config = require("config");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;
const fs = require("fs");
const moment = require("moment");

const USERNAME = config.get("elasticsearch.username");
const PASSWORD = config.get("elasticsearch.password");
const ELASTICSEARCH_URL = config.get("elasticsearch.url");
const INDEX_START_DATE = config.get("elasticsearch.indexStartDate");// dev沒有7/23~7/16 index
const INDEX_END_DATE = config.get("elasticsearch.indexEndDate");
const K8S_NAMESPACE = config.get("elasticsearch.k8sNamespace");
const TYPE = "_doc";
const SEARCH_KEYWORDS = config.get("elasticsearch.searchKeyword");
const RESPONSE_FIELD = ["log"];
const ELASTICSEARCH_PAGE_SIZE = 10000;

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0"; // disable SSL in axios

const NODE_ENV = process.env.NODE_ENV;
console.log("NODE_ENV:", NODE_ENV);
const OUTPUT_FILENAME_PREFIX = `output_${NODE_ENV}/`;
const OUTPUT_CSV_HEADER = [
    { id: "log", title: "Log" }
];

function createFolder(localOutputFolder) {
    if (!fs.existsSync(localOutputFolder)) {
        fs.mkdirSync(localOutputFolder);
    }
}

function getIndex({startDate, endDate, namespace}) {
    const dates = [];
    const startDateMoment = moment.utc(startDate);
    const endDateMoment = moment.utc(endDate);
    while (!startDateMoment.isAfter(endDateMoment)) {
        dates.push(`${namespace}-${startDateMoment.format("YYYY.MM.DD")}`);
        startDateMoment.add(1, "day");
    }
    return dates;
}

async function sendRequest({ index, type, from = null}) {
    const url = `${ELASTICSEARCH_URL}/${index}/${type}/_search`;
    const auth = {
        username: USERNAME,
        password: PASSWORD
    };
    const body = {
        ...(from ? { from } : {}), // start from 0
        size: ELASTICSEARCH_PAGE_SIZE,
        query: {
            match: {
                message: SEARCH_KEYWORDS
            }
        },
        fields: RESPONSE_FIELD,
        sort: [{"@timestamp" : {"order" : "asc"}}],
        "_source": false
    };
    return await axios.post(url, body, { auth });
}

async function getDataFromEs({ index, type = TYPE, from }) {
    const sendRequestRes = await sendRequest({ index, type, from });

    const totalCount = sendRequestRes.data.hits.total.value;

    const data = sendRequestRes.data.hits.hits.map((item) => {
        return {
            log: item.fields.log[0].replace(/(\r\n|\n|\r)/gm, "")
        };
    });
    return { totalCount, data };
}

async function writeCsvPromise({ index, data, append = false }) {
    const csvWriter = createCsvWriter({
        path: `${OUTPUT_FILENAME_PREFIX}${index}.csv`,
        header: OUTPUT_CSV_HEADER,
        append
    });
    return csvWriter.writeRecords(data);
}


async function main() {
    createFolder(OUTPUT_FILENAME_PREFIX);

    const indexes = getIndex({ startDate: INDEX_START_DATE, endDate: INDEX_END_DATE, namespace: K8S_NAMESPACE });
    // const indexes = ["infra-external-2022.08.26"];
    console.log("indexes:", indexes);

    for (const index of indexes) {
        const csvWriterPromises = [];
        let from = 0;
    
        const { totalCount, data } = await getDataFromEs({ index, from });
        console.log(`index: ${index}, totalCount: ${totalCount}, process start`);
    
        csvWriterPromises.push(writeCsvPromise({ index, data }));
    
        from +=ELASTICSEARCH_PAGE_SIZE;
        while (from < totalCount) {
            // console.log("from:", from);
    
            const { data } = await getDataFromEs({ index, from });
    
            csvWriterPromises.push(writeCsvPromise({ index, data, append: true }));
    
            from +=ELASTICSEARCH_PAGE_SIZE;
        }
        Promise.all(csvWriterPromises);
        console.log(`index: ${index}, totalCount: ${totalCount}, process finish`);
    }
}
main();
