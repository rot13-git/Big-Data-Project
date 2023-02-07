var source = new EventSource('/stream');

function addRow(data){
    var tableRow = document.getElementById("table_tweet");
    var row = tableRow.insertRow(1);
    var cell1 = row.insertCell(0);
    var cell2 = row.insertCell(1);
    var cell3 = row.insertCell(2);

    cell1.innerHTML = '<div><a href="https://twitter.com/i/web/status/'+data["id"]+'" target="_blank">'+data["id"]+'</a></div>';
    cell2.innerHTML = '<div>'+unescape(data["tweet"]).slice(2)+'</div>';
    //cell3.innerHTML = data["latitude"];
    if(data["sentiment"].includes("Negative")){
        cell3.innerHTML = '<div style="color:red;">'+data["sentiment"]+'</div>';
    }
    else if(data["sentiment"].includes("Positive")){
        cell3.innerHTML = '<div style="color:green;">'+data["sentiment"]+'</div>';
    }
    else{
        cell3.innerHTML = '<div>'+data["sentiment"]+'</div>';
    }
    
}

source.onmessage = function (event) {
    parseData(event);
  };

function stopStream(){
    this.source.close();
}

function startStream(){
    this.source = new EventSource('/stream');
    source.onmessage = function (event) {
       parseData(event);
    };
}

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
          "id": id,
          "latitude": latitude,
          "longitude": longitude
        };
        addRow(data);
}

