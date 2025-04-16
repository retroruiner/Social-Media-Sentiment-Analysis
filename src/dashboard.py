import os
import json
import base64
from io import BytesIO

import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
from wordcloud import WordCloud
import requests

# You can still read your API_BASE_URL from environment
API_BASE_URL = os.environ.get("API_BASE_URL", "/api")


def init_dashboard(server):
    # Initialize the Dash app with the given Flask server
    app = dash.Dash(
        __name__,
        server=server,  # Pass the Flask server instance here!
        external_stylesheets=[dbc.themes.BOOTSTRAP],
    )
    app.title = "Social Media Sentiment Dashboard"

    # Define the layout for your dashboard
    app.layout = dbc.Container(
        [
            html.H1(
                "Social Media Sentiment Dashboard",
                className="text-center text-primary my-4 fw-bold",
            ),
            # Input for on-demand analysis (optional)
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Input(
                            id="query",
                            type="text",
                            placeholder="Search topic...",
                            className="form-control",
                        ),
                        md=8,
                    ),
                    dbc.Col(
                        html.Button(
                            "Analyze Now",
                            id="fetch-button",
                            n_clicks=0,
                            className="btn btn-success w-100",
                        ),
                        md=4,
                    ),
                ],
                className="mb-4",
            ),
            # Interval component to automatically refresh daily summary data (update every 24 hours)
            dcc.Interval(
                id="daily-summary-interval",
                interval=24 * 60 * 60 * 1000,
                n_intervals=0,
            ),
            # Hidden div to store daily summary data
            html.Div(id="daily-summary-store", style={"display": "none"}),
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Graph(
                            id="sentiment-distribution",
                            config={"displayModeBar": False},
                        ),
                        md=6,
                    ),
                    dbc.Col(
                        dcc.Graph(
                            id="sentiment-over-time", config={"displayModeBar": False}
                        ),
                        md=6,
                    ),
                ],
                className="mb-4",
            ),
            dbc.Row(
                dbc.Col(
                    html.Div(
                        [
                            html.H5("Word Cloud", className="text-center mb-2"),
                            html.Img(
                                id="wordcloud",
                                style={
                                    "width": "100%",
                                    "border-radius": "10px",
                                    "box-shadow": "0 2px 6px rgba(0,0,0,0.2)",
                                },
                            ),
                        ]
                    ),
                    width=12,
                ),
                className="mb-4",
            ),
            dbc.Row(
                dbc.Col(
                    dcc.Graph(
                        id="top-words-positive", config={"displayModeBar": False}
                    ),
                    width=12,
                ),
                className="mb-4",
            ),
            dbc.Row(
                dbc.Col(
                    dcc.Graph(
                        id="top-words-negative", config={"displayModeBar": False}
                    ),
                    width=12,
                ),
                className="mb-4",
            ),
            dbc.Row(
                dbc.Col(
                    dcc.Graph(
                        id="heatmap-sentiment-time", config={"displayModeBar": False}
                    ),
                    width=12,
                ),
                className="mb-5",
            ),
        ],
        fluid=True,
    )

    # (Include your callbacks below as defined in your current code.)
    @app.callback(
        [
            dash.Output("sentiment-distribution", "figure"),
            dash.Output("sentiment-over-time", "figure"),
            dash.Output("wordcloud", "src"),
            dash.Output("heatmap-sentiment-time", "figure"),
            dash.Output("top-words-positive", "figure"),
            dash.Output("top-words-negative", "figure"),
        ],
        [dash.Input("fetch-button", "n_clicks")],
        [dash.State("query", "value")],
    )
    def update_graphs(n_clicks, query):
        if n_clicks == 0:
            return (dash.no_update,) * 6
        posts_response = requests.get(
            f"{API_BASE_URL}/fetch_posts?query={query}"
        ).json()
        if "error" in posts_response:
            return (dash.no_update,) * 6

        analysis_response = requests.post(
            f"{API_BASE_URL}/analyze_data", json={"posts": posts_response}
        ).json()

        # Process analysis_response to build your charts as before...
        sentiment_df = pd.DataFrame(
            analysis_response["sentiment_distribution"].items(),
            columns=["Sentiment", "Count"],
        )
        sentiment_fig = px.pie(
            sentiment_df,
            names="Sentiment",
            values="Count",
            title="Sentiment Distribution",
            color="Sentiment",
            color_discrete_map={"POSITIVE": "green", "NEGATIVE": "red"},
        )

        sentiment_time_df = pd.DataFrame(analysis_response["sentiment_over_time"])
        x_col = "date" if "date" in sentiment_time_df else "hour"
        if x_col == "hour":
            sentiment_time_df["hour"] = sentiment_time_df["hour"].apply(
                lambda h: f"{int(h):02d}:00"
            )
        else:
            sentiment_time_df["date"] = pd.to_datetime(
                sentiment_time_df["date"], errors="coerce"
            )
            sentiment_time_df["date"] = sentiment_time_df["date"].dt.strftime(
                "%Y-%m-%d"
            )

        sentiment_time_fig = px.line(
            sentiment_time_df,
            x=x_col,
            y="count",
            color="sentiment",
            title="Sentiment Over Time",
            color_discrete_map={"POSITIVE": "green", "NEGATIVE": "red"},
            markers=True,
        )
        sentiment_time_fig.update_layout(
            xaxis_tickangle=-45,
            xaxis_tickfont=dict(size=10),
            margin=dict(t=40, b=80),
            legend_title=None,
        )

        # Generate Word Cloud
        word_freq = analysis_response["word_frequency"]
        wc = WordCloud(
            width=900, height=400, background_color="black"
        ).generate_from_frequencies(word_freq)
        img = BytesIO()
        wc.to_image().save(img, format="PNG")
        wordcloud_src = (
            "data:image/png;base64," + base64.b64encode(img.getvalue()).decode()
        )

        heatmap_df = pd.DataFrame(analysis_response["heatmap_data"]).T.fillna(0)
        ordered_days = [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ]
        heatmap_df = heatmap_df.reindex(ordered_days)
        heatmap_fig = px.imshow(
            heatmap_df,
            labels={"x": "Hour", "y": "Day", "color": "Posts"},
            aspect="auto",
            title="Posts Volume by Day and Hour",
            color_continuous_scale="Viridis",
        )

        top_words = analysis_response["top_words_by_sentiment"]
        pos_words_df = pd.DataFrame(
            top_words["POSITIVE"].items(), columns=["Word", "Count"]
        )
        neg_words_df = pd.DataFrame(
            top_words["NEGATIVE"].items(), columns=["Word", "Count"]
        )

        pos_words_fig = px.bar(
            pos_words_df.sort_values(by="Count"),
            x="Count",
            y="Word",
            orientation="h",
            title="Top Positive Words",
            color_discrete_sequence=["green"],
        )
        neg_words_fig = px.bar(
            neg_words_df.sort_values(by="Count"),
            x="Count",
            y="Word",
            orientation="h",
            title="Top Negative Words",
            color_discrete_sequence=["red"],
        )

        return (
            sentiment_fig,
            sentiment_time_fig,
            wordcloud_src,
            heatmap_fig,
            pos_words_fig,
            neg_words_fig,
        )

    @app.callback(
        dash.Output("daily-summary-store", "children"),
        [dash.Input("daily-summary-interval", "n_intervals")],
    )
    def update_daily_summary(n_intervals):
        try:
            resp = requests.get(f"{API_BASE_URL}/daily_summary")
            if resp.status_code == 200:
                daily_summary_data = resp.json()
                return json.dumps(daily_summary_data)
            else:
                return dash.no_update
        except Exception as e:
            print(f"Error fetching daily summary: {e}")
            return dash.no_update

    return app
