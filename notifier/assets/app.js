var data = [];
var myChart = echarts.init(document.getElementById("container"));

option = {
    title: {
        text: 'Crawler Monitor'
    },
    legend: {
        data:['监控数据']
    },
    tooltip: {
        trigger: 'axis',
        axisPointer: {
            animation: false
        }
    },
    xAxis: {
        type: 'time',
        splitLine: {
            show: false
        }
    },
    yAxis: {
        type: 'value',
        boundaryGap: [0, '100%'],
        splitLine: {
            show: true
        }
    },
    series: [{
        name: '监控数据',
        type: 'line',
        showSymbol: false,
        hoverAnimation: false,
        data: data
    }]
};

ws = new WebSocket("ws://localhost:8000/progress");
ws.onopen = function(evt) {
    console.info("connected");
    myChart.setOption(option, true);
};
ws.onmessage = function(evt) {

    // console.log(evt.data);

    if (data.length > 200) {
        data.shift();
    }
    data.push(filter(evt.data));

    myChart.setOption({
        series: [{
            data: data
        }]
    });
};
ws.onerror = function(evt) {
    console.error(evt);
};

function filter(message) {
    var data = JSON.parse(message);
    return {
        name: "abc",
        value: [ data["Elapsed"], data["AirportIndex"] ]
    }
}