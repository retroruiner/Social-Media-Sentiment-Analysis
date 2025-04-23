import os
import datetime
from flask import Flask
import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
from wordcloud import WordCloud
import base64
from io import BytesIO

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Post, Base  # your SQLAlchemy models + metadata
from data_processor import DataProcessor

# ─── DATABASE SETUP ─────────────────────────────────────────────────────────
DATABASE_URL = os.environ["DATABASE_URL"]
engine = create_engine(DATABASE_URL, echo=False, future=True)
Session = sessionmaker(bind=engine)
# Create tables if they don’t exist (or run migrations separately)
Base.metadata.create_all(bind=engine)

# ─── FLASK + DASH SETUP ─────────────────────────────────────────────────────
server = Flask(__name__)
app = dash.Dash(
    __name__,
    server=server,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    title="Social Media Sentiment Dashboard",
)

# ─── LAYOUT ─────────────────────────────────────────────────────────────────
app.layout = dbc.Container(
    [
        # Hidden interval component triggers update every hour
        dcc.Interval(id="interval-component", interval=3600000, n_intervals=0),  # 1h
        html.H1(
            "Social Media Sentiment Dashboard",
            className="text-center text-primary my-4 fw-bold",
        ),
        # Search input (fires callback on change)
        dbc.Row(
            dbc.Col(
                dcc.Input(
                    id="query",
                    type="text",
                    placeholder="Search topic...",
                    className="form-control",
                ),
                width=12,
            ),
            className="mb-4",
        ),
        # Charts
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
                dcc.Graph(id="top-words-positive", config={"displayModeBar": False}),
                width=12,
            ),
            className="mb-4",
        ),
        dbc.Row(
            dbc.Col(
                dcc.Graph(id="top-words-negative", config={"displayModeBar": False}),
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


# ─── CALLBACK ───────────────────────────────────────────────────────────────
@app.callback(
    [
        dash.Output("sentiment-distribution", "figure"),
        dash.Output("sentiment-over-time", "figure"),
        dash.Output("wordcloud", "src"),
        dash.Output("heatmap-sentiment-time", "figure"),
        dash.Output("top-words-positive", "figure"),
        dash.Output("top-words-negative", "figure"),
    ],
    [
        dash.Input("interval-component", "n_intervals"),
        dash.Input("query", "value"),
    ],
)
def update_graphs(n_intervals, query):
    # Load posts from the database
    session = Session()
    q = session.query(Post)
    if query:
        q = q.filter(Post.text.ilike(f"%{query}%"))
    db_posts = q.all()
    session.close()

    if not db_posts:
        return (dash.no_update,) * 6

    # Prepare data
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

    # 1) Sentiment distribution pie
    sent_df = pd.DataFrame(
        proc.get_sentiment_distribution().items(), columns=["Sentiment", "Count"]
    )
    sentiment_fig = px.pie(
        sent_df,
        names="Sentiment",
        values="Count",
        title="Sentiment Distribution",
        color="Sentiment",
        color_discrete_map={"POSITIVE": "green", "NEGATIVE": "red"},
    )

    # 2) Sentiment over time
    sto_df = pd.DataFrame(proc.aggregate_sentiment_by_date().to_dict("records"))
    if "hour" in sto_df:
        sto_df["hour"] = sto_df["hour"].astype(int)
        hours = sorted(sto_df["hour"].unique())
        sentiment_time_fig = px.line(
            sto_df,
            x="hour",
            y="count",
            color="sentiment",
            title="Sentiment Over Time",
            color_discrete_map={"POSITIVE": "green", "NEGATIVE": "red"},
            markers=True,
        )
        sentiment_time_fig.update_layout(
            xaxis=dict(
                tickmode="array",
                tickvals=hours,
                ticktext=[f"{h:02d}:00" for h in hours],
                tickangle=-45,
                tickfont=dict(size=10),
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
            color_discrete_map={"POSITIVE": "green", "NEGATIVE": "red"},
            markers=True,
        )
        sentiment_time_fig.update_layout(
            xaxis_tickangle=-45,
            xaxis_tickfont=dict(size=10),
            margin=dict(t=40, b=80),
            legend_title=None,
        )

    # 3) Word cloud
    freqs = proc.get_word_frequency()
    if freqs:
        wc = WordCloud(
            width=900, height=400, background_color="black"
        ).generate_from_frequencies(freqs)
        img = BytesIO()
        wc.to_image().save(img, format="PNG")
        wordcloud_src = (
            "data:image/png;base64," + base64.b64encode(img.getvalue()).decode()
        )
    else:
        wordcloud_src = ""

    # 4) Heatmap of post volume
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
        color_continuous_scale="Viridis",
    )

    # 5) Top positive & negative words
    tw = proc.get_top_words_by_sentiment()
    pos_df = pd.DataFrame(tw["POSITIVE"].items(), columns=["Word", "Count"])
    neg_df = pd.DataFrame(tw["NEGATIVE"].items(), columns=["Word", "Count"])
    pos_words_fig = px.bar(
        pos_df.sort_values("Count"),
        x="Count",
        y="Word",
        orientation="h",
        title="Top Positive Words",
        color_discrete_sequence=["green"],
    )
    neg_words_fig = px.bar(
        neg_df.sort_values("Count"),
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


# ─── RUN ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8050)),
    )
