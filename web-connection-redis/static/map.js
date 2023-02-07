var source = new EventSource('/stream');

var width = 900;
var height = 600;

var projection = d3.geo.mercator()
    .center([0, 5 ]);

var svg = d3.select("body").append("svg")
    .attr("width", width)
    .attr("height", height);
var path = d3.geo.path()
    .projection(projection);
var g = svg.append("g");

d3.json("/static/world-110m2.json", function(error, topology) {
    g.selectAll("path")
      .data(topojson.object(topology, topology.objects.countries)
          .geometries)
    .enter()
      .append("path")
      .attr("d", path)
});

function drawMarker(data) {
  
  var color = "white";
  if(data["sentiment"].includes("Positive")) {
    color = "blue";
  } else if(data["sentiment"].includes("Neutral")) {
    color = "red";
  }

  var g = svg.append("g");
  var circleElement = g.selectAll("circle")
  .data([data])
  .enter()
  .append("circle")
  .attr("cx", function(d) {
    console.log(d);
    return projection([d.longitude, d.latitude])[0];
  })
  .attr("cy", function(d) {
    return projection([d.longitude, d.latitude])[1];
  })
  .attr("r", 5)
  .style("fill", color);

  circleElement.append("svg:title")
   .text(function(d) { return d.tweet; });

}

source.onmessage = function (event) {
  data = event.data.split("|");
  
  coordinates = JSON.parse(data[1]);
  tweet = data[0];
  sentiment = data[2];
  //console.log(coordinates[0][0]);
  latitude = parseFloat(coordinates[0][0][0]);
  longitude = parseFloat(coordinates[0][0][1]);
  data = {
    "tweet": tweet,
    "sentiment": sentiment,
    "latitude": latitude,
    "longitude": longitude
  };
  console.log(data);
  drawMarker(data);
};