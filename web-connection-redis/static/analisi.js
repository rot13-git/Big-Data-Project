var source = new EventSource('/stream');

var source_analysis = new EventSource('/analysis_stream');
// chart colors
var colors = ['#157347','#ff0000','#DDCEB7'];

var colors_device = ['#EC9857','#2FD37D','#0075A4'];

var colors_hashtag = ['#00546B','#6A5E42','#99481E','#63D4D8','#FBB7D3','#007148','#DF3890','#FF6020','#FEC462','#AB1026'];

/* 3 donut charts */
var donutOptions = {
  cutoutPercentage: 30, 
  legend: {position:'bottom', padding:5, labels: {pointStyle:'circle', usePointStyle:true}}
};
var barOptions = {
  legend: {
    display: false //This will do the task
 }
}
var chLineOptions = {
  scales: {
    x:[
      {
        type:'time',
      }
    ]
  }
}

source.onmessage = function (event) {
  parseData(event);
};
function stopStream(){
  source.close();
  source_analysis.close();
}

source_analysis.onmessage = function (event) {
  parseDataAnalysis(event);
}

function parseDataAnalysis(event){
  var data = event.data.slice(2,-1).split("|");
  if(data[0].includes("TIME")){
    updateTweetTime(data[1],data[2]);
  }
  else{
    updateHashtagChart(data[0],data[1]);
  }
}
function startStream(){
  source = new EventSource('/stream');
  source.onmessage = function (event) {
     parseData(event);
  };
  source_analysis = new EventSource('/analysis_stream');
  source_analysis.onmessage = function (event) {
    parseDataAnalysis(event);
  }
}
function parseData(event){
  var test = event.data.split("|");
  var device = test[4];
  var sentiment = test[3];
  updateSentimentChart(sentiment);

  updateDeviceChart(device);
}
// donut 1
var chDonutData1 = {
    labels: ['Positive', 'Negative', 'Neutral'],
    datasets: [
      {
        backgroundColor: colors.slice(0,3),
        borderWidth: 0,
        data: [0, 0, 0]
      }
    ]
};

var chDonutData2 = {
  labels: ['Iphone', 'Android', 'Others'],
  datasets: [
    {
      backgroundColor: colors_device.slice(0,3),
      borderWidth: 0,
      data: [0, 0, 0]
    }
  ]
};

var chDonutData3 = {
  labels: [],
  datasets:[
    {
      backgroundColor: colors_hashtag.slice(0,10),
      borderWidth: 0,
      data: []
    }
  ]
}

var chLineData = {
  labels:[],
  datasets:[
    {
      data:[],
      borderColor: "#ff6384",
      backgroundColor: "#FFA0B4",
      pointStyle: 'circle',
      pointRadius: 10,
      pointHoverRadius: 15,
      label:"Tweet al Minuto"
    }
  ]
}
function updateSentimentChart(sentiment){
  if(sentiment.includes("Positive")){
    sentimentChart.data.datasets[0].data[0]= sentimentChart.data.datasets[0].data[0]+1;
  }
  if(sentiment.includes("Neutral")){
    sentimentChart.data.datasets[0].data[2]= sentimentChart.data.datasets[0].data[2]+1;
  }
  else{
    sentimentChart.data.datasets[0].data[1]= sentimentChart.data.datasets[0].data[1]+1;
  }
  sentimentChart.update();
}

function updateDeviceChart(device){
  if(device.includes("iPhone")){
    deviceChart.data.datasets[0].data[0]= deviceChart.data.datasets[0].data[0]+1;
  }
  if(device.includes("Android")){
    deviceChart.data.datasets[0].data[2]= deviceChart.data.datasets[0].data[2]+1;
  }
  else{
    deviceChart.data.datasets[0].data[1]= deviceChart.data.datasets[0].data[1]+1;
  }
  //console.log(deviceChart);
  deviceChart.update();
}

function updateHashtagChart(label,count){
  var topFiveLabel = label.slice(1,-1).split(",");
  var topFiveCount = count.slice(1,-1).split(",");
  hashtagChart.data.labels = topFiveLabel;
  hashtagChart.data.datasets[0].data = topFiveCount;
  //console.log(hashtagChart.labels);
  hashtagChart.update();
}

function updateTweetTime(label,count){
  var label_elab = label.slice(1,-1).split(",");
  
  tweetTime.data.labels = label_elab;
  console.log(tweetTime);
  tweetTime.data.datasets[0].data = count.slice(1,-1).split(",");
  tweetTime.update();
}
var chDonut1 = document.getElementById("sentimentChart");
var sentimentChart;
if (chDonut1) {
  sentimentChart = new Chart(chDonut1, {
      type: 'pie',
      data: chDonutData1,
      options: donutOptions
  });
}

var chDonut2 = document.getElementById("deviceChart");
var deviceChart;
if(chDonut2){
  deviceChart = new Chart(chDonut2,{
    type: 'pie',
    data: chDonutData2,
    options: donutOptions
  });
}

var chDonut3 = document.getElementById("hashtagChart");
var hashtagChart;
if(chDonut3){
  hashtagChart = new Chart(chDonut3,{
    type: 'bar',
    data: chDonutData3,
    options: barOptions
  })
}

var chLine = document.getElementById("tweetTime");
var tweetTime;
if(chLine){
  tweetTime = new Chart(chLine,{
    type: 'line',
    data: chLineData,
    options: chLineOptions
  });
}
document.getElementById("start_button").addEventListener("click",startStream);
document.getElementById("stop_button").addEventListener("click",stopStream);


