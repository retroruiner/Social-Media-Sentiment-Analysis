import os
import datetime
from flask import Flask
import dash
from dash import dcc, html, Input, Output, State, callback_context
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
from wordcloud import WordCloud
import base64
from io import BytesIO

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.models import Post, Base
from src.data_processor import DataProcessor

from dash.exceptions import PreventUpdate

# ─── CACHE SETUP ────────────────────────────────────────────────────────────
data_cache = {"signature": None, "outputs": None}

# ─── DATABASE SETUP ─────────────────────────────────────────────────────────
DATABASE_URL = os.environ["DATABASE_URL"]
engine = create_engine(DATABASE_URL, echo=False, future=True)
Session = sessionmaker(bind=engine)
Base.metadata.create_all(bind=engine)

# ─── FLASK + DASH SETUP ─────────────────────────────────────────────────────
server = Flask(__name__)
app = dash.Dash(__name__, server=server, external_stylesheets=[dbc.themes.BOOTSTRAP])

# ─── LAYOUT ─────────────────────────────────────────────────────────────────
app.layout = dbc.Container(
    fluid=True,
    children=[
        dcc.Interval(
            id="interval-component",
            interval=6 * 60 * 60 * 1000,  # every 6 hours
            n_intervals=0,
        ),
        html.H1(
            "Social Media Sentiment Dashboard",
            className="text-center my-4 fw-bold",
            style={"color": "teal"},
        ),
        dbc.Row(
            [
                dbc.Col(
                    dcc.Input(
                        id="add-query-input",
                        type="text",
                        placeholder="Enter new query…",
                        className="form-control",
                    ),
                    md=4,
                ),
                dbc.Col(
                    html.Button(
                        "Add Query",
                        id="add-query-button",
                        n_clicks=0,
                        className="btn btn-success w-100",
                    ),
                    md=2,
                ),
                dbc.Col(
                    html.Div(
                        [
                            html.Label(
                                "Current Query (in use)", className="fw-semibold"
                            ),
                            dcc.Input(
                                id="current-query-display",
                                type="text",
                                value="",
                                disabled=True,
                                className="form-control",
                            ),
                        ]
                    ),
                    md=3,
                ),
                dbc.Col(
                    html.Div(
                        [
                            html.Label(
                                "Next Query (after next fetch)", className="fw-semibold"
                            ),
                            dcc.Input(
                                id="next-query-display",
                                type="text",
                                value="",
                                disabled=True,
                                className="form-control",
                            ),
                        ]
                    ),
                    md=3,
                ),
            ],
            className="mb-4",
            align="end",
        ),
        dbc.Row(
            [
                dbc.Col(
                    dcc.Graph(
                        id="sentiment-distribution", config={"displayModeBar": False}
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
            [
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
            ],
            className="mb-4",
        ),
        dbc.Row(
            [
                dbc.Col(
                    dcc.Graph(
                        id="top-words-positive", config={"displayModeBar": False}
                    ),
                    width=12,
                ),
            ],
            className="mb-4",
        ),
        dbc.Row(
            [
                dbc.Col(
                    dcc.Graph(
                        id="top-words-negative", config={"displayModeBar": False}
                    ),
                    width=12,
                ),
            ],
            className="mb-4",
        ),
        dbc.Row(
            [
                dbc.Col(
                    dcc.Graph(
                        id="heatmap-sentiment-time", config={"displayModeBar": False}
                    ),
                    width=12,
                ),
            ],
            className="mb-5",
        ),
    ],
)


@app.callback(
    [
        Output("current-query-display", "value"),
        Output("next-query-display", "value"),
    ],
    [
        Input("add-query-button", "n_clicks"),
        Input("interval-component", "n_intervals"),
    ],
    [State("add-query-input", "value")],
)
def manage_query(n_clicks, _, new_query_value):
    trigger = callback_context.triggered[0]["prop_id"].split(".")[0]
    session = Session()

    latest_posts = session.query(Post).order_by(Post.id.desc()).limit(2).all()
    current_query = latest_posts[0].query if latest_posts else ""

    latest_marker = (
        session.query(Post)
        .filter(Post.uri == "query")
        .order_by(Post.created_at.desc())
        .first()
    )
    next_query = latest_marker.query if latest_marker else ""

    if trigger == "add-query-button":
        if not new_query_value:
            session.close()
            raise PreventUpdate

        post = Post(
            uri="query",
            text="pending-query",
            sentiment="UNKNOWN",
            confidence=0.0,
            query=new_query_value,
        )
        session.add(post)
        session.commit()
        session.close()

        data_cache["signature"] = None
        return current_query, new_query_value

    session.close()
    return current_query, next_query


@app.callback(
    [
        Output("sentiment-distribution", "figure"),
        Output("sentiment-over-time", "figure"),
        Output("wordcloud", "src"),
        Output("heatmap-sentiment-time", "figure"),
        Output("top-words-positive", "figure"),
        Output("top-words-negative", "figure"),
    ],
    [Input("interval-component", "n_intervals")],
)
def update_graphs(n_intervals):
    session = Session()
    db_posts = session.query(Post).filter(Post.uri != "query").all()
    session.close()

    if not db_posts:
        return (dash.no_update,) * 6

    timestamps = [p.created_at for p in db_posts if p.created_at]
    latest_ts = max(timestamps) if timestamps else None
    signature = (len(db_posts), latest_ts)

    if signature == data_cache["signature"]:
        return data_cache["outputs"]

    posts = [
        {
            "uri": p.uri,
            "text": p.text,
            "sentiment": p.sentiment,
            "confidence": p.confidence,
            "createdAt": p.created_at.isoformat() if p.created_at else None,
        }
        for p in db_posts
    ]
    proc = DataProcessor(posts)

    sent_df = pd.DataFrame(
        proc.get_sentiment_distribution().items(), columns=["Sentiment", "Count"]
    )
    sentiment_fig = px.pie(
        sent_df,
        names="Sentiment",
        values="Count",
        color="Sentiment",
        title="Sentiment Distribution",
        color_discrete_map={"POSITIVE": "green", "NEGATIVE": "red"},
    )

    sto = proc.aggregate_sentiment_by_date()
    sto_df = pd.DataFrame(sto.to_dict("records") if hasattr(sto, "to_dict") else sto)
    if "hour" in sto_df:
        sto_df["hour"] = sto_df["hour"].astype(int)
        hours = sorted(sto_df["hour"].unique())
        sentiment_time_fig = px.line(
            sto_df,
            x="hour",
            y="count",
            color="sentiment",
            title="Sentiment Over Time",
            markers=True,
            color_discrete_map={"POSITIVE": "green", "NEGATIVE": "red"},
        )
        sentiment_time_fig.update_layout(
            xaxis=dict(
                tickmode="array",
                tickvals=hours,
                ticktext=[f"{h:02d}:00" for h in hours],
                tickangle=-45,
            ),
            margin=dict(t=40, b=80),
            legend_title=None,
        )
    else:
        sto_df["date"] = pd.to_datetime(sto_df["date"], errors="coerce").dt.strftime(
            "%Y-%m-%d"
        )
        sentiment_time_fig = px.line(
            sto_df,
            x="date",
            y="count",
            color="sentiment",
            title="Sentiment Over Time",
            markers=True,
            color_discrete_map={"POSITIVE": "green", "NEGATIVE": "red"},
        )
        sentiment_time_fig.update_layout(
            xaxis_tickangle=-45, margin=dict(t=40, b=80), legend_title=None
        )

    freqs = proc.get_word_frequency()
    if freqs:
        wc = WordCloud(width=600, height=300, background_color="black", max_words=100)
        img = wc.generate_from_frequencies(freqs).to_image()
        buf = BytesIO()
        img.save(buf, format="PNG")
        wordcloud_src = (
            "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode()
        )
    else:
        wordcloud_src = ""

    hm_df = pd.DataFrame(proc.get_heatmap_data()).T.fillna(0)
    ordered_days = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    hm_df = hm_df.reindex(ordered_days)
    heatmap_fig = px.imshow(
        hm_df,
        labels={"x": "Hour", "y": "Day", "color": "Posts"},
        aspect="auto",
        title="Posts Volume by Day and Hour",
    )

    tw = proc.get_top_words_by_sentiment()
    pos_df = pd.DataFrame(tw["POSITIVE"].items(), columns=["Word", "Count"])
    neg_df = pd.DataFrame(tw["NEGATIVE"].items(), columns=["Word", "Count"])
    pos_words_fig = px.bar(
        pos_df.sort_values("Count"),
        x="Count",
        y="Word",
        orientation="h",
        title="Top Positive Words",
    )
    pos_words_fig.update_traces(marker_color="green")
    neg_words_fig = px.bar(
        neg_df.sort_values("Count"),
        x="Count",
        y="Word",
        orientation="h",
        title="Top Negative Words",
    )
    neg_words_fig.update_traces(marker_color="red")

    data_cache["signature"] = signature
    data_cache["outputs"] = (
        sentiment_fig,
        sentiment_time_fig,
        wordcloud_src,
        heatmap_fig,
        pos_words_fig,
        neg_words_fig,
    )
    return data_cache["outputs"]


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8050)), debug=False)
