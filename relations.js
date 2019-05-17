
let http = require('https');
let gunzip = require('zlib').createGunzip();
let readline = require('readline');
let fs = require('fs');

var dir = __dirname + '/data';
if (!fs.existsSync(dir)){
    fs.mkdirSync(dir);
}

let url = 'https://datasets.imdbws.com/title.principals.tsv.gz';
let csvFilepath = __dirname + '/data/title.principals.csv';
let csvHeaders = ':START_ID,role:string[],:END_ID,:TYPE \n';


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
                let movieID = values[0];
                let personID = values[2];
                let category = values[3];
                let char = values[5].replace('\\N', ' ');

                if(char !== ' ') {
                    let nchar = '"';
                    eval(char).forEach(c => {
                        nchar += c
                            .replace(/\[/g, '(')
                            .replace(/\]/g, ')')
                            .replace(/"/g, '""');

                    })
                    char = nchar + '"';
                }

                let rel = '';
                switch(category) {
                    case 'self':
                    case 'actor':
                    case 'actress':
                        rel = 'ACTED_IN';
                        break;

                    case 'director':
                        rel = 'DIRECTED';
                        break;
                 
                    case 'composer':
                        rel = 'COMPOSED_FOR';
                        break;

                    case 'producer':
                        rel = 'PRODUCED';
                        break;
                    
                    case 'editor':
                        rel = 'EDITED';
                        break;

                    case 'cinematographer':
                        rel = 'FILMED';
                        break;

                    case 'writer':
                        rel = 'WROTE';
                        break;
                }

                if(rel !== '') {
                    stream.write(`${personID},${char},${movieID},${rel} \n`);
                }

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
