var sql = require("mssql");

var config = {
    user: 'nodejsteam2',
    password: 'HelloWorld!',
    server: 'reports-db.cfgf8dz2qitj.eu-central-1.rds.amazonaws.com', 
    database: 'dbo.reports-db' 
};

exports.handler = async (event, context, callback) => {
    try {
        let pool = await sql.connect(config);
        let orders = await pool.request()
            .query('select * from Orders');
            
        console.log(orders);
        callback(null, "success!");
    } catch (err) {
        console.log("error!");
        console.log(err);
        callback(err);
    }
};

//exports.handler(null, null, null).then();