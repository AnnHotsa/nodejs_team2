var express = require('express')
var app = express();
var port = process.env.PORT || 3000;

app.get('/', (req, res) => {
    res.send("success!");
})

app.listen(port, function() {
   console.log('Server is listening on port: ' + port);
});
