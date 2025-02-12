<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{{ nb.metadata.name }}</title>
    <style>
        /* Basic styling */
        body { font-family: sans-serif; }
        pre { background-color: #f5f5f5; padding: 10px; }
        /* Style for images */
        img { max-width: 100%; height: auto; }
        /* Style for tables */
        table { border-collapse: collapse; }
        th, td { border: 1px solid black; padding: 5px; }
    </style>
    <!-- Include Plotly.js -->
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
</head>
<body>
{% for cell in nb.cells %}
    {%- if cell.cell_type == 'markdown' %}
        {{ cell.source | markdown2html }}
    {%- elif cell.cell_type == 'code' and cell.outputs %}
        {% for output in cell.outputs %}
            {% if 'application/vnd.plotly.v1+json' in output.data %}
                <div id="plotly_{{ loop.index }}"></div>
                <script>
                var plotlyData_{{ loop.index }} = {{ output.data['application/vnd.plotly.v1+json'] | tojson | safe }};
                Plotly.newPlot('plotly_{{ loop.index }}', plotlyData_{{ loop.index }}.data, plotlyData_{{ loop.index }}.layout);
                </script>
            {% elif 'text/html' in output.data %}
                {{ output.data['text/html'] | safe }}
            {% elif 'image/png' in output.data %}
                <img src="data:image/png;base64,{{ output.data['image/png'] | base64encode }}">
            {% elif 'image/jpeg' in output.data %}
                <img src="data:image/jpeg;base64,{{ output.data['image/jpeg'] | base64encode }}">
            {% elif 'application/pdf' in output.data %}
                <iframe src="data:application/pdf;base64,{{ output.data['application/pdf'] | base64encode }}" width="100%" height="500px"></iframe>
            {% elif 'text/plain' in output.data %}
                <pre>{{ output.data['text/plain'] }}</pre>
            {% else %}
                <pre>{{ output.text | escape if 'text' in output else "" }}</pre>
            {% endif %}
        {% endfor %}
    {% endif %}
{% endfor %}
</body>
</html>