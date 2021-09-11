const axios = require("axios")
const cheerio = require("cheerio")
const cluster = require('cluster')
const mysql      = require('mysql')
const util = require('util')
const { performance } = require('perf_hooks')
const fs = require('fs-extra')
const { uuid } = require('uuidv4');

// (async () => {
//     const connection = mysql.createConnection({
//         host:'google-account.cmlfk75xsv3h.ap-south-1.rds.amazonaws.com', 
//         user: 'shahrushabh1996', 
//         database: 'rapidTax',
//         password: '11999966',
//         ssl: 'Amazon RDS'
//     })
    
//     const query = util.promisify(connection.query).bind(connection)

//     const connections = new Array(65).fill(0)

//     for (let [index, connection] of connections.entries()) {
//         try {
//             const trademarks = await query(`SELECT * FROM trademarks WHERE status = 'PROCESSING' OR status = 'FAILED' ORDER BY id ASC LIMIT 2000`)
    
//             console.log('trademarks Fetched')
    
//             const trademarkIds = []
    
//             trademarks.map((trademark) => trademarkIds.push(trademark.id))
    
//             console.log('Trademarks ID array ready')
    
//             if (trademarkIds.length) {
//                 await query(`UPDATE trademarks SET status = ? WHERE id IN (?)`, ['NOT STARTED', trademarkIds])
//             }
    
//             console.log('Updating status')
//         } catch (e) {
//             console.error(e)
//         }
//     }

//     connection.destroy()

//     process.exit()
// })()

async function fetchHTML(url) {
    const { data } = await axios.get(url)
    return cheerio.load(data)
}

(async () => {
    function timeout(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    if (cluster.isMaster) {
        console.log(`Primary ${process.pid} is running`);

        const connections = new Array(15).fill(0)

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
        (async () => {
            const connection = mysql.createConnection({
                host:'google-account.cmlfk75xsv3h.ap-south-1.rds.amazonaws.com', 
                user: 'shahrushabh1996', 
                database: 'rapidTax',
                password: '11999966',
                ssl: 'Amazon RDS'
            })
    
            const query = util.promisify(connection.query).bind(connection)
    
            let trademarks
    
            try {
                console.log('Transaction begin')
    
                await connection.beginTransaction()
    
                trademarks = await query(`SELECT * FROM trademarks WHERE status = 'NOT STARTED' ORDER BY id ASC LIMIT 2000`)
    
                // trademarks = await query(`SELECT * FROM trademarks WHERE Name = 'A'`)
    
                console.log('Trademarks Fetched')
    
                const trademarkIds = []
    
                trademarks.map((trademark) => trademarkIds.push(trademark.id))
    
                console.log('Trademark ID array ready')
    
                if (trademarkIds.length) {
                    await query(`UPDATE trademarks SET status = ? WHERE id IN (?)`, ['PROCESSING', trademarkIds])
                }
    
                console.log('Updating status')
    
                await connection.commit()
            } catch(err) {
                console.log(err)
                connection.rollback()
            }
    
            for (let trademark of trademarks) {
                if (trademark.URL === null) continue
                const t0 = performance.now()
    
                try {
                    console.log(`${trademark.URL} ::: MAIN ::: Fetching HTML`)

                    const $ = await fetchHTML(trademark.URL)

                    const allTrademarks = []
    
                    let fetchedTrademerks = await saveTrademarks($)
                    
                    allTrademarks.push(...fetchedTrademerks)
    
                    if ($('.pager-div').length) {
                        const spliitedUrl = $('.pager-div a').last().attr('href').split('-')
                        const totalPages = parseInt(spliitedUrl[spliitedUrl.length - 1])
                        fetchedTrademerks = await pagination(trademark.URL, totalPages)
                        allTrademarks.push(...fetchedTrademerks)
                    }

                    if (allTrademarks.length) await query('INSERT INTO trademarkData (id, Name, Class, ApplicantName, ApplicantLink, ApplicantId, Logo, ApplicationDate, Status, Description, Address) VALUES ?',
                    [allTrademarks.map(trademark => [uuid(), trademark.Name, trademark.Class, trademark.ApplicantName, trademark.ApplicantLink, trademark.ApplicantId, trademark.Logo, trademark.ApplicationDate, trademark.Status, trademark.Description, trademark.Address])])

                    await query(`UPDATE trademarks SET status = ? WHERE id IN (?)`, ['SUCCESS', trademark.id])

                    const t1 = performance.now()

                    console.log(`${trademark.URL} ::: SUCCESS Time took ${((t1 - t0) / 1000)} seconds.`)
                } catch (err) {
                    const t1 = performance.now()
    
                    await query(`UPDATE trademarks SET status = ? WHERE id IN (?)`, ['FAILED', trademark.id])
    
                    console.log(err)
    
                    console.log(`${trademark.URL} ::: FAILED Time took ${((t1 - t0) / 1000)} seconds.`)
                }
            }

            connection.destroy()

            process.exit()
        })()

        async function saveTrademarks($) {
            const trademarks = []

            $('.main-wrapper').each(function() {
                const ele = $(this).prev()

                trademarks.push({
                    Name: $(ele).find('span').eq(0).text().replace('Trademark : ', ''),
                    Class: $(ele).find('span').eq(1).text().replace('Class : ', ''),
                    ApplicantName: $(this).find('label').eq(0).next().eq(0).text(),
                    ApplicantLink: $(this).find('label').eq(0).next().eq(0).attr('href'),
                    ApplicantId: $(this).find('label').eq(0).next().eq(0).attr('href').split('/').pop(),
                    Logo: $(this).find('img').eq(0).attr('src'),
                    ApplicationDate: $(this).find('label').eq(1)[0].nextSibling.nodeValue,
                    Status: $(this).find('label').eq(2)[0].nextSibling.nodeValue,
                    Description: $(this).find('label').eq(3)[0].nextSibling.nodeValue,
                    Address: $(this).find('label').eq(4)[0].nextSibling.nodeValue
                })
            })

            return trademarks
        }

        async function pagination(URL, totalPages) {
            const allTrademarks = []
            const pages = new Array(totalPages).fill(0)
            for (let [index, page] of pages.entries()) {
                if (index + 1 < 2) continue
                console.log(`${URL}/page-${index + 1} ::: SUB ::: Fetching HTML`)
                const $ = await fetchHTML(`${URL}/page-${index + 1}`)
                const pageTrademarks = await saveTrademarks($)
                allTrademarks.push(...pageTrademarks)
            }
            return allTrademarks
        }
    }
})()