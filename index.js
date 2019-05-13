var sql = require("mssql");
var AWS = require('aws-sdk');
var s3 = new AWS.S3();

var config = {
    user: 'nodejsteam2',
    password: 'HelloWorld!',
    server: 'reports-db.cfgf8dz2qitj.eu-central-1.rds.amazonaws.com', 
    database: 'dbo.reports-db' 
};

exports.handler = async (event, context, callback) => {
    try {
        let pool = await sql.connect(config);
        let employeeSalesByCountry = (await pool.request()
            .query("select distinct b.*, a.CategoryName from Categories a"
            + " inner join Products b on a.CategoryID = b.CategoryID "
            + "where b.Discontinued = 0 order by b.ProductName;")).recordset;

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
            
        console.log(employeeSalesByCountry);
        callback(null, "success!");
    } catch (err) {
        console.log("error!");
        console.log(err);
        callback(err);
    }
};

//exports.handler(null, null, null).then();