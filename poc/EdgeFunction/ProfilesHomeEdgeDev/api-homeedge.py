import json
CONTENT = """
<!doctype html><html lang="en"><head><meta charset="utf-8"/><link rel="icon" href="/favicon.png"/><meta name="viewport" content="width=device-width,initial-scale=1"/><meta name="theme-color" content="#000000"/><meta name="description" content="Web site created using create-react-app"/><link rel="apple-touch-icon" href="/logo192.png"/><link rel="stylesheet" href="https://use.typekit.net/rdz2isl.css"/><link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css" integrity="sha384-ggOyR0iXCbMQv3Xipma34MD+dH/1fQ784/j6cY/iJTQUOhcWr7x9JvoRxT2MZw1T" crossorigin="anonymous"/><link rel="manifest" href="/manifest.json"/><link rel="canonical" href="SITE_URL_VALUE"> <meta property="og:locale" content="en_US"/> <meta property="og:type" content="website"/> <meta property="og:title" content="Generic profile image scraping"/> <meta property="og:description" content="This is the POC for scraping user's generic profile image" /> <meta property="og:url" content="SITE_URL_VALUE" /> <meta property="og:site_name" content="Test POC" /> <meta property="fb:app_id" content="399889854017941" /> <meta property="og:image" content="http://54.145.134.223/generic_1200_630.png" /> <meta property="og:image:type" content="image/png" /> <meta property="og:image:width" content="1200" /> <meta property="og:image:height" content="630" /><title>Interpersonality</title><link href="/static/css/2.9de105d4.chunk.css" rel="stylesheet"><link href="/static/css/main.967643eb.chunk.css" rel="stylesheet"></head><body><noscript>You need to enable JavaScript to run this app.</noscript><div id="root"></div><script src="https://js.chargebee.com/v2/chargebee.js"></script><script src="https://unpkg.com/infinite-scroll@3/dist/infinite-scroll.pkgd.min.js"></script><script src="https://cdn.jsdelivr.net/npm/lazyload@2.0.0-rc.2/lazyload.js"></script><script>!function(o){o(document).ready((function(){o("#dropdown-notifications, #dropdown-settings").on("show.bs.dropdown",(function(d){o('<div class="dropdown-backdrop fade"></div>').appendTo("body"),o("div.dropdown-backdrop").addClass("show")})),o("#dropdown-notifications, #dropdown-settings").on("hide.bs.dropdown",(function(d){o("div.dropdown-backdrop").removeClass("show").remove()}))}))}(jQuery)</script><script>!function(e){function r(r){for(var n,i,l=r[0],a=r[1],p=r[2],c=0,s=[];c<l.length;c++)i=l[c],Object.prototype.hasOwnProperty.call(o,i)&&o[i]&&s.push(o[i][0]),o[i]=0;for(n in a)Object.prototype.hasOwnProperty.call(a,n)&&(e[n]=a[n]);for(f&&f(r);s.length;)s.shift()();return u.push.apply(u,p||[]),t()}function t(){for(var e,r=0;r<u.length;r++){for(var t=u[r],n=!0,l=1;l<t.length;l++){var a=t[l];0!==o[a]&&(n=!1)}n&&(u.splice(r--,1),e=i(i.s=t[0]))}return e}var n={},o={1:0},u=[];function i(r){if(n[r])return n[r].exports;var t=n[r]={i:r,l:!1,exports:{}};return e[r].call(t.exports,t,t.exports,i),t.l=!0,t.exports}i.m=e,i.c=n,i.d=function(e,r,t){i.o(e,r)||Object.defineProperty(e,r,{enumerable:!0,get:t})},i.r=function(e){"undefined"!=typeof Symbol&&Symbol.toStringTag&&Object.defineProperty(e,Symbol.toStringTag,{value:"Module"}),Object.defineProperty(e,"__esModule",{value:!0})},i.t=function(e,r){if(1&r&&(e=i(e)),8&r)return e;if(4&r&&"object"==typeof e&&e&&e.__esModule)return e;var t=Object.create(null);if(i.r(t),Object.defineProperty(t,"default",{enumerable:!0,value:e}),2&r&&"string"!=typeof e)for(var n in e)i.d(t,n,function(r){return e[r]}.bind(null,n));return t},i.n=function(e){var r=e&&e.__esModule?function(){return e.default}:function(){return e};return i.d(r,"a",r),r},i.o=function(e,r){return Object.prototype.hasOwnProperty.call(e,r)},i.p="/";var l=this.webpackJsonpinterpersonality=this.webpackJsonpinterpersonality||[],a=l.push.bind(l);l.push=r,l=l.slice();for(var p=0;p<l.length;p++)r(l[p]);var f=a;t()}([])</script><script src="/static/js/2.92cd0005.chunk.js"></script><script src="/static/js/main.50b3b84b.chunk.js"></script></body></html>
"""
 
def handler(event, context):
    # Generate HTTP OK response using 200 status code with HTML body. uri
    print("event::::",event) 
    # request = event['Records'][0]['cf']['request']
    # print("request:::",request)
    #SITE_URL = "http://d244etbmd76r93.cloudfront.net/index.html"
    SITE_URL = "https://" + event['Records'][0]['cf']['config']['distributionDomainName'] + event['Records'][0]['cf']['request']['uri']
    print(SITE_URL)

    response = {
      'status': '200',
      'statusDescription': 'OK',
      'headers': {
           'cache-control': [
               {
                   'key': 'Cache-Control',
                   'value': 'max-age=100'
               }
           ],
           "content-type": [
               {
                   'key': 'Content-Type',
                   'value': 'text/html'
               }
           ]
       },
      'body': CONTENT.replace('SITE_URL_VALUE', SITE_URL)
    }
    print("response",response)
    return response