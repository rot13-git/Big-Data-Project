var source = new EventSource('/stream');

var map = L.map('map').setView([51.505, 51 ], 3);
L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
}).addTo(map);

function drawMarker(data){
    color_selected = "blue";
    if(data["sentiment"].includes("Negative")){
        color_selected = "red";
    }
    if(data["sentiment"].includes("Positive")){
        color_selected = "green";
    }
    if(data["sentiment"].includes("Neutral")){
        color_selected = "blue";
    }
    var circle = L.circle([data.latitude, data.longitude], {
        color: color_selected,
        fillColor: '#f03',
        fillOpacity: 0.5,
        radius: 150
    }).addTo(map);

    circle.bindPopup(unescape(data["tweet"]).slice(2)+'<div><a href="https://twitter.com/i/web/status/'+data["id"]+'" target="_blank">'+"Link"+'</a></div>');
}

function stopStream(){
    this.source.close();
}

function startStream(){
    this.source = new EventSource('/stream');
    source.onmessage = function (event) {
       parseData(event);
    };
}
function getColor(d) {
    return d === 'Negative'  ? "#ff0000" :
           d === 'Positive'  ? "#00ff00" :
           d === 'Neutral' ? "#0040FF" : "#ff7f00";
                        
}


var legend = L.control({position: 'bottomleft'});
legend.onAdd = function(map){
    var div = L.DomUtil.create('div','legend');
    var labels = ["Positive","Neutral","Negative"];
    div.innerHTML = '<div class="legend"><b>Legend</b></div>';
    for(var i=0;i<labels.length;i++){
        div.innerHTML+='<i style="background:'+getColor(labels[i])+'"">&nbsp;</i>&nbsp;&nbsp;'+labels[i]+'<br/>';
    }
    return div;
}

legend.addTo(map);

function parseData(event){
    data = event.data.split("|");
    coordinates = JSON.parse(data[1]);
    tweet = data[0];
    id = data[2];
    sentiment = data[3];
    //console.log(coordinates[0][0]);
    latitude = parseFloat(coordinates[0][0][1]);
    longitude = parseFloat(coordinates[0][0][0]);
    data = {
      "tweet": tweet,
      "sentiment": sentiment,
      "latitude": latitude,
      "longitude": longitude,
      "id": id
    };
    //console.log(data);
    drawMarker(data);
}

source.onmessage = function (event) {  
    parseData(event);
  };
