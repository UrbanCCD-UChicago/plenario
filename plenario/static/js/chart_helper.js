var ChartHelper = {};
ChartHelper.chart = function(el, title, source, time_agg, data, iteration) {
  
  return new Highcharts.Chart({
      chart: {
          renderTo: el + "_" + iteration,
          type: 'line'
      },
      title: {
          text: title
      },
      legend: {
        enabled: false
      },
      subtitle: {
          text: 'Source: ' + source
      },
      xAxis: {
          title: {
              text: 'Date'
          },
          type: 'datetime'
      },
      yAxis: {
          title: {
              text: 'Count'
          }
      },
      tooltip: {
          formatter: function() {
              return '<b>'+ this.series.name +'</b><br/>'+
                  ChartHelper.toolTipDateFormat(time_agg, this.x) +': '+ this.y;
          }
      },
      plotOptions: {
          series: {
            marker: {
              radius: 0,
              states: {
                hover: {
                  enabled: true,
                  radius: 5
                }
              }
            },
            shadow: false,
            states: {
               hover: {
                  lineWidth: 3
               }
            }
          }
      },
      series: [{
          color: ChartHelper.colors[iteration],
          name: title,
          data: data
      }]
  });
}

ChartHelper.sparkline = function(el, time_agg, data) {
  
  // console.log(el);
  // console.log(time_agg);
  // console.log(data);
  return new Highcharts.Chart({
      chart: {
          renderTo: el,
          type: 'area'
      },
      title: {
          text: ''
      },
      legend: {
        enabled: false
      },
      credits: {
          enabled: false
      },
      xAxis: {
          title: {
              text: ''
          },
          type: 'datetime',
          labels: {
            enabled: false
          }
      },
      yAxis: {
          title: {
              text: ''
          }
      },
      tooltip: {
          formatter: function() {
              return ChartHelper.toolTipDateFormat(time_agg, this.x) +': '+ this.y;
          }
      },
      plotOptions: {
          series: {
            marker: {
              fillColor: "#518fc9",
              radius: 0,
              states: {
                hover: {
                  enabled: true,
                  radius: 5
                }
              }
            },
            shadow: false,
            states: {
               hover: {
                  lineWidth: 3
               }
            }
          }
      },
      series: [{
          data: data,
          lineColor: "#518fc9",
          color: "#ddf2fb"
      }]
  });
}

ChartHelper.toolTipDateFormat = function(interval, x) {
  if (interval == "year" || interval == "decade")
    return Highcharts.dateFormat("%Y", x);
  if (interval == "quarter")
    return Highcharts.dateFormat("%B %Y", x);
  if (interval == "month")
    return Highcharts.dateFormat("%B %Y", x);
  if (interval == "week")
    return Highcharts.dateFormat("%b %e, %Y", x);
  if (interval == "day")
    return Highcharts.dateFormat("%b %e, %Y", x);
  if (interval == "hour")
    return Highcharts.dateFormat("%H:00", x);
  else
    return 1;
}

ChartHelper.colors = ["#A6761D", "#7570B3", "#D95F02", "#66A61E", "#E7298A", "#E6AB02", "#1B9E77"];