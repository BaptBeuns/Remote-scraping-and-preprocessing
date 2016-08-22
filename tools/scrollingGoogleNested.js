var system = require('system');
var webPage = require('webpage');
var fs = require('fs');

var page = webPage.create();
var args = system.args;

page.settings.userAgent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/45.0.2454.85 Chrome/45.0.2454.85 Safari/537.36';
page.settings.resourceTimeout = 60000;
page.viewportSize = {width: 1920, height: 10000};

var outputFilename = args[1]
var request = args[2]
// var request = args.slice(2, args.length).join('+');
// var fileName = args.slice(2, args.length).join('_');
console.log("START:", "filename", outputFilename)
console.log(outputFilename)

console.log('OPEN: http://www.google.com/search?tbm=isch&q=' + request)

page.open('http://www.google.com/search?tbm=isch&q=' + request, function() {

    var count = 0;
    console.log("START:", "scrolling", outputFilename)
    window.setTimeout(function() {
        window.setInterval(function () {
            if (count < 5) {
                console.log("SCROLL:", "step", count)
                page.evaluate(function() {
                    window.document.body.scrollTop = document.body.scrollHeight;
                });
                page.evaluate(function() {
                    var t_0 = document.getElementById('smc');
                    if (Object.keys(t_0).length > 0) {
                        var t_1 = document.getElementById('smc').getElementsByTagName('div');
                        if (t_1.length > 0) {
                            var t_2 = document.getElementById('smc').getElementsByTagName('div')[0].getElementsByTagName('input');
                            if (t_2.length > 0) {
                                var a = document.getElementById('smc').getElementsByTagName('div')[0].getElementsByTagName('input')[0];
                                var e  = document.createEvent('MouseEvents');
                                e.initMouseEvent('click', true, true, window, 0, 0, 0, 0, 0, false, false, false, false, 0, null);
                                a.dispatchEvent(e);
                            }
                        }
                    }
                    return t_0;
                });
                count++;
            }
            if (count == 5) {
                console.log('Start timeout');
                count++;
                window.setTimeout(function() {
                    console.log('The end');
                    //page.render(outputDir + '/' + fileName + '/google_final.jpg');
                    fs.write(outputFilename, page.content, 'w')
                    phantom.exit();
                }, Math.floor(Math.random() * 1500) + 8000);
            }
        }, Math.floor(Math.random() * 1500) + 8000);
    }, Math.floor(Math.random() * 1500) + 8000);
});
