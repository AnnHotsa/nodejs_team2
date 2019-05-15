var sql = require("mssql");
var AWS = require('aws-sdk');
var s3 = new AWS.S3();

var config = {
    user: 'nodejsteam2',
    password: 'HelloWorld!',
    server: 'reports-db.cfgf8dz2qitj.eu-central-1.rds.amazonaws.com',
    database: 'dbo.reports-db'
};

function twoDigits(d) {
    if(0 <= d && d < 10) return "0" + d.toString();
    if(-10 < d && d < 0) return "-0" + (-1*d).toString();
    return d.toString();
}

function dateToSQL(date) {
    return date.getUTCFullYear() + "-" + twoDigits(1 + date.getUTCMonth()) + "-" + twoDigits(date.getUTCDate()) + " "
    + twoDigits(date.getUTCHours()) + ":" + twoDigits(date.getUTCMinutes()) + ":" + twoDigits(date.getUTCSeconds());
};

function writeReportMetadata(report) {
    let query = `INSERT INTO ReportsMetadata (S3Location, ReportFrom, ReportTo, CreatedDate)
            VALUES (${report.S3Location}, ${dateToSQL(report.ReportFrom)}, ${dateToSQL(report.ReportTo)}, ${dateToSQL(report.CreatedDate)})`;
    let pool = await sql.connect(config);
    let response = await pool.request()
            .query(query);

}

exports.handler = async (event, context, callback) => {
    try {
        let fromDate = new Date(event.fromDate * 1000);
        let toDate = new Date(event.toDate * 1000);
        console.log(`Message received from queue. From date: ${fromDate}, To date: ${toDate}`);

        let query = `SELECT o.ShipCountry,E.EmployeeID, SUM(ODE.ExtendedPrice) FROM Orders O
            INNER JOIN Employees E ON O.EmployeeID = E.EmployeeID
            INNER JOIN [Order Details Extended] ODE ON o.OrderID = ODE.OrderID
            WHERE O.OrderDate BETWEEN ${dateToSQL(fromDate)} AND ${dateToSQL(toDate)}
            GROUP BY O.ShipCountry, E.EmployeeID"`;
        let pool = await sql.connect(config);
        let employeeSalesByCountry = await pool.request()
            .query(query);

        var bucketName = process.env.bucketName;
        var date = new Date().toJSON().slice(0,10).replace(/-/g,'_');
        var keyName = "jsonReports/report_" + date + ".json";
        var content = JSON.stringify(employeeSalesByCountry);
    
        var params = { Bucket: bucketName, Key: keyName, Body: content };
    
        s3.putObject(params, function (err, data) {
            if (err)
                console.log(err)
            else
                console.log("Successfully saved object to " + bucketName + "/" + keyName);
        });
            //Write to SQL
            let reportMetadata = {
                S3Location: `https://s3-eu-central-1.amazonaws.com/${bucketName}/${keyName}`,
                CreatedDate: new Date(),
                ReportFrom: fromDate,
                ReportTo: toDate
            }
            writeReportMetadata(reportMetadata);

        console.log(employeeSalesByCountry);
        callback(null, "success!");
    } catch (err) {
        console.log("error!");
        console.log(err);
        callback(err);
    }
};

//exports.handler(null, null, null).then();