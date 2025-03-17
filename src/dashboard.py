import requests
import dash
from dash import dcc, html
import plotly.express as px
import dash_bootstrap_components as dbc
import pandas as pd
from wordcloud import WordCloud
import base64
from io import BytesIO

# Flask API URL
API_BASE_URL = "http://127.0.0.1:5000"

# Initialize Dash app with Bootstrap for better styling
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Layout
app.layout = dbc.Container(
    [
        html.H1("Social Media Sentiment Analysis", className="text-center mb-4"),
        # Search Input & Button
        dbc.Row(
            [
                dbc.Col(
                    dcc.Input(
                        id="query",
                        type="text",
                        placeholder="Enter search term...",
                        className="form-control",
                    ),
                    width=8,
                ),
                dbc.Col(
                    html.Button(
                        "Fetch Data",
                        id="fetch-button",
                        n_clicks=0,
                        className="btn btn-primary",
                    ),
                    width=4,
                ),
            ],
            className="mb-4",
        ),
        # Graphs Layout
        dbc.Row(
            [
                dbc.Col(dcc.Graph(id="sentiment-distribution"), width=6),
                dbc.Col(dcc.Graph(id="sentiment-over-time"), width=6),
            ],
            className="mb-4",
        ),
        dbc.Row(
            [
                dbc.Col(dcc.Graph(id="text-length-sentiment"), width=6),
                dbc.Col(
                    html.Img(
                        id="wordcloud", style={"width": "100%", "border-radius": "10px"}
                    ),
                    width=6,
                ),
            ],
            className="mb-4",
        ),
    ],
    fluid=True,
)


@app.callback(
    [
        dash.Output("sentiment-distribution", "figure"),
        dash.Output("sentiment-over-time", "figure"),
        dash.Output("text-length-sentiment", "figure"),
        dash.Output("wordcloud", "src"),
    ],
    [dash.Input("fetch-button", "n_clicks")],
    [dash.State("query", "value")],
)
def update_graphs(n_clicks, query):
    if n_clicks == 0:
        return dash.no_update

    # Fetch posts from API
    posts_response = requests.get(f"{API_BASE_URL}/fetch_posts?query={query}").json()
    if "error" in posts_response:
        return dash.no_update

    # Analyze data
    analysis_response = requests.post(
        f"{API_BASE_URL}/analyze_data", json={"posts": posts_response}
    ).json()

    # Pie Chart for Sentiment Distribution (Green for Positive, Red for Negative)
    sentiment_distribution_df = pd.DataFrame(
        list(analysis_response["sentiment_distribution"].items()),
        columns=["Sentiment", "Count"],
    )
    sentiment_colors = {"POSITIVE": "green", "NEGATIVE": "red"}
    sentiment_fig = px.pie(
        sentiment_distribution_df,
        names="Sentiment",
        values="Count",
        title="Sentiment Distribution",
        color="Sentiment",
        color_discrete_map=sentiment_colors,
    )

    # Line Chart for Sentiment Over Time (Red for Negative, Green for Positive)
    sentiment_time_df = pd.DataFrame(analysis_response["sentiment_over_time"])
    print("Sentiment Over Time Data:")
    print(sentiment_time_df)

    # Check which column is available for the x-axis
    if "date" in sentiment_time_df.columns:
        sentiment_time_df["date"] = pd.to_datetime(
            sentiment_time_df["date"], errors="coerce"
        )
        x_col = "date"
        title = "Sentiment Over Time"
    elif "hour" in sentiment_time_df.columns:
        # If only 'hour' exists, use it for aggregation
        x_col = "hour"
        title = "Sentiment Over Hour"
    else:
        # Fallback in case neither exists
        x_col = None
        title = "Sentiment Over Time"

    # Build the line chart only if we have a valid x-axis column
    if x_col:
        time_fig = px.line(
            sentiment_time_df,
            x=x_col,
            y="count",
            color="sentiment",
            title=title,
            color_discrete_map={"POSITIVE": "green", "NEGATIVE": "red"},
        )
    else:
        time_fig = {}

    # Scatter Plot for Text Length vs Sentiment
    length_df = pd.DataFrame(analysis_response["text_length_sentiment"])
    length_fig = px.scatter(
        length_df,
        x="text_length",
        y="net_sentiment",
        title="Text Length vs Sentiment",
        color="net_sentiment",
        color_continuous_scale=px.colors.sequential.Viridis,
    )

    # Word Cloud Generation
    word_freq = analysis_response["word_frequency"]
    print("Word Frequency:")
    print(word_freq)
    wordcloud = WordCloud(
        width=900,
        height=400,
        background_color="black",
        colormap="Set2",
        contour_color="white",
        contour_width=2,
        max_words=100,
    ).generate_from_frequencies(word_freq)

    img = BytesIO()
    wordcloud.to_image().save(img, format="PNG")
    wordcloud_src = "data:image/png;base64,{}".format(
        base64.b64encode(img.getvalue()).decode()
    )

    return sentiment_fig, time_fig, length_fig, wordcloud_src


# Run Dash App
if __name__ == "__main__":
    app.run(debug=True)
