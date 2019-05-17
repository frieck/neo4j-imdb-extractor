
let http = require('https');
let gunzip = require('zlib').createGunzip();
let readline = require('readline');
let fs = require('fs');

var dir = __dirname + '/data';
if (!fs.existsSync(dir)){
    fs.mkdirSync(dir);
}

let url = 'https://datasets.imdbws.com/name.basics.tsv.gz';
let csvFilepath = __dirname + '/data/name.basics.csv';
let csvHeaders = 'personId:ID,name,birth:int,death:int,:LABEL \n';


let stream = fs.createWriteStream(csvFilepath);
stream.once('open', function () {
    http.get(url, response => {
        let len = parseInt(response.headers['content-length'], 10);
        let cur = 0;
        var total = len / 1048576; //1048576 - bytes in  1Megabyte

        response.on('data', function (chunk) {
            cur += chunk.length;
            process.stdout.cursorTo(0);
            process.stdout.write('Downloading ' + (100.0 * cur / len).toFixed(2) + '%    ' + (cur / 1048576).toFixed(2) + 'mb of Total size: ' + total.toFixed(2) + 'mb');
        });

        response.on('end', function () {
            console.log('\nDownloading complete');
        });

        let lineReader = readline.createInterface({
            input: response.pipe(gunzip)
        });

        let n = 0;
        lineReader.on('line', (line) => {
            
            if (n === 0) {
                stream.write(csvHeaders);
            } else {
                let values = line.split('\t');
                let personID = values[0];
                let name = values[1].replace(/"/g, '""');
                let birth = values[2].replace('\\N', ' ');
                let death = values[3].replace('\\N', ' ');

                stream.write(`${personID},"${name}",${birth},${death},Person \n`);
            }
            n++;
        });

        lineReader.on('close', () => {
            console.log(n + ' lines processed.');
            stream.end();
        });

    }).on('error', function (e) {
        console.log('Error: ' + e.message);
    });
});
