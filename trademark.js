const axios = require("axios")
const cheerio = require("cheerio")
const { performance } = require('perf_hooks')
const { uuid } = require('uuidv4')
const mysql      = require('mysql')
const util = require('util')
const cluster = require('cluster')

async function fetchHTML(url) {
    const { data } = await axios.get(url)
    return cheerio.load(data)
}

// (async () => {
//     const connection = mysql.createConnection({
//         host:'127.0.0.1', 
//         user: 'root', 
//         database: 'RapidTax',
//         password: '11999966'
//     })

//     const query = util.promisify(connection.query).bind(connection)

//     const alphabets = ["A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"];

//     for (let alpha of alphabets) {
//         const t0 = performance.now()

//         const $ = await fetchHTML(`https://www.zaubacorp.com/trademarkbrowse/${alpha}`)

//         const pages = []

//         for (let i = 1; i <= parseInt($('#block-system-main > div > div > div.text-right').text().split('of')[1].replace(/\,/g,"")); i++) {
//             pages.push({
//                 id: uuid(),
//                 URL: `https://www.zaubacorp.com/trademarkbrowse/${alpha}/${i}`
//             })
//         }

//         await query('INSERT INTO trademarkPages (id, URL) VALUES ?',
//             [pages.map(page => [page.id, page.URL])])

//         const t1 = performance.now()

//         console.log(`Alphabet ::: ${alpha} SUCCESS Time took ${((t1 - t0) / 1000)} seconds.`)
//     }
// })()

// (async () => {
//     const pages = new Array(200).fill(0)

//     for (let [index, page] of pages.entries()) {
//         const t0 = performance.now()

//         const $ = await fetchHTML(`https://www.zaubacorp.com/trademarkbrowse/a/${index+1}`)

//         const rows = []
//         const $table = $("table")
    
//         $table.find("tbody tr").each(function () {
//             var row = {}
    
//             $(this).find("td").each(function (i) {
//                 row['name'] = $(this).text()
//                 row['URL'] = $(this).find('a').attr('href')
//             })
    
//             rows.push(row)
//         })

//         const t1 = performance.now()
    
//         console.log(`${index+1} ::: https://www.zaubacorp.com/trademarkbrowse/a/${index+1} ::: SUCCESS Time took ${((t1 - t0) / 1000)} seconds.`)
//     }
// })()

(async () => {
    function timeout(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    if (cluster.isMaster) {
        console.log(`Primary ${process.pid} is running`);

        const connections = new Array(1).fill(0)

        for (let [index, connection] of connections.entries()) {
            await launchCluster()
        }

        async function launchCluster() {
            const clusterLaunch = cluster.fork()
            clusterLaunch.on('exit', async (worker, code, signal) => {
                console.log(`worker died`);
    
                await launchCluster()
            });
            await timeout(10000)
            return
        }
    } else {
        const connection = mysql.createConnection({
            host:'127.0.0.1', 
            user: 'root', 
            database: 'RapidTax',
            password: '11999966'
        })

        const query = util.promisify(connection.query).bind(connection)

        let trademarkPages

        try {
            console.log('Transaction begin')

            await connection.beginTransaction()

            trademarkPages = await query(`SELECT * FROM trademarkPages WHERE status = 'NOT STARTED' ORDER BY id ASC LIMIT 706`)

            console.log('trademarkPages Fetched')

            const trademarkPagesIds = []

            trademarkPages.map((trademarkPage) => trademarkPagesIds.push(trademarkPage.id))

            console.log('trademarkPagesIds array ready')

            if (trademarkPages.length) {
                await query(`UPDATE trademarkPages SET status = ? WHERE id IN (?)`, ['PROCESSING', trademarkPagesIds])
            }

            console.log('Updating status')

            await connection.commit()
        } catch (err) {
            console.log(err)
            connection.rollback()
        }

        for (let trademarkPage of trademarkPages) {
            const t0 = performance.now()

            try {
                const $ = await fetchHTML(trademarkPage.URL)

                const trademarks = []
                const $table = $("table")
            
                $table.find("tbody tr").each(function () {
                    var row = {}
            
                    $(this).find("td").each(function (i) {
                        row['Name'] = $(this).text()
                        row['URL'] = $(this).find('a').attr('href')
                    })
            
                    trademarks.push(row)
                })

                await query('INSERT INTO trademarks (id, Name, URL) VALUES ?',
                [trademarks.map(trademark => [uuid(), trademark.Name, trademark.URL])])

                await query(`UPDATE trademarkPages SET status = ? WHERE id IN (?)`, ['SUCCESS', trademarkPage.id])

                const t1 = performance.now()

                console.log(`${trademarkPage.URL} ::: SUCCESS Time took ${((t1 - t0) / 1000)} seconds.`)
            } catch (err) {
                console.log(err)

                await query(`UPDATE trademarkPages SET status = ? WHERE id IN (?)`, ['FAILED', trademarkPage.id])

                const t1 = performance.now()

                console.log(`${trademarkPage.URL} ::: FAILED Time took ${((t1 - t0) / 1000)} seconds.`)
            }
        }
    }
})()