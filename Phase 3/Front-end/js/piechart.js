/**
 * Created by bn4n5 on 12/3/2016.
 */

google.charts.load('current', {'packages':['corechart']});
google.charts.setOnLoadCallback(drawChart);

function drawChart() {
    //API call
    var data_file = "https://api.mlab.com/api/1/databases/pbdb/collections/query5?apiKey=quJa8qCv_KGUvY5S3Qnf9EDnWzoDvSQA";
    var http_request = new XMLHttpRequest();

    http_request.onreadystatechange = function(){

        if (http_request.readyState == 4  ){
            var jsonObj = JSON.parse(http_request.responseText);
            var data = new google.visualization.DataTable();
            data.addColumn('string', 'Time Zone');
            data.addColumn('number', 'Number of Tweets');
            for(var i=0;i<jsonObj.length;i++)
            {
                data.addRows([
                    [jsonObj[i].time_zone, jsonObj[i].users]
                ]);
            }
            // Set chart options
            var options = {
                'title':"Pie Chart",
                'width':1000,
                'height':450};

            var chart = new google.visualization.PieChart(document.getElementById('pie_chart'));
            chart.draw(data, options);
        }
    }

    http_request.open("GET", data_file, true);
    http_request.send();
}