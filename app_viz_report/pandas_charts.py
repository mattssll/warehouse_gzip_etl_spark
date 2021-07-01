import psycopg2
import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from matplotlib import pyplot as plt; plt.rcdefaults()
from matplotlib import ticker
import squarify
import numpy as np
from pdf_mail import sendpdf

absolutePathToApp = "/Users/mateus.leao/Documents/mattssll/takeaway"


def generate_viz_report(email_list: [], sender_email: str, sender_password: str) -> None:
    # Connect to postgresql
    def get_connection():
        engine = create_engine('postgresql+psycopg2://postgres:admin@127.0.0.1:5432/dwh_takeaway', pool_recycle=3600);
        con = engine.connect();
        return con

    def query_postgresql(query: str) -> DataFrame:
        con = get_connection()
        df = pd.read_sql(query, con);
        return df

    def plot_charts(df: DataFrame, type: str, subplot_rows: int, subplot_cols: int, chart_pos: int,
                    hasAxis: str, title: str, chartH: int, chartW: int) -> None:
        plt.gca().yaxis.set_major_formatter(plt.matplotlib.ticker.StrMethodFormatter('{x:,.0f}'))
        fig = plt.gcf()
        ax = fig.add_subplot()
        fig.set_size_inches(chartW, chartH)
        plt.subplot(subplot_rows, subplot_cols, chart_pos)
        if type == "treemap":
            squarify.plot(sizes=df["count"], label=df["category"].str.slice(0, 7), alpha=0.6)
        elif type == "histogram":
            plt.hist(dfHistogram["price"], bins=30, alpha=0.5)
            plt.ticklabel_format(useOffset=False, style='plain')
            plt.grid()
        elif type == "bar":
            y_pos = np.arange(len(df.iloc[:, 0]))
            plt.style.context('bmh')
            plt.bar(y_pos, df["count"], align="center", alpha=0.5)
            plt.xticks(y_pos, [str(item)[0:23] for item in df.iloc[:, 0]])
            plt.ticklabel_format(axis="y", style='plain')
        plt.axis(hasAxis)
        plt.title(title)

    try:
        subplot_rows = 4
        subplot_cols = 1
        # Queries to get charts
        query_ratings_hist = "select rating, count(*) from public.fct_reviews group by rating order by count(*) desc"
        query_price_hist = "select price from public.dim_products where price > 0 and price < 150"
        query_category_treemap = "select category, count(*) from public.dim_products where category not like 'None' " \
                                 "group by category order by count(*) desc limit 10 "
        query_category_bar = "select category, count(*) from public.dim_products where category not like 'None' group by category order by count(*) desc limit 5"
        print("working on review ratings viz")
        # Plot 1
        dfCountRatings = query_postgresql(query_ratings_hist)
        plot_charts(df=dfCountRatings, type="bar", subplot_rows=subplot_rows, subplot_cols=subplot_cols, chart_pos=1, hasAxis="on",
                    title="Distribution of Price Rating Reviews", chartW=12, chartH=16)
        print("working on price histogram viz")
        dfHistogram = query_postgresql(query_price_hist)
        # Plot 2
        plot_charts(df=dfHistogram, type="histogram", subplot_rows=subplot_rows, subplot_cols=subplot_cols, chart_pos=2, hasAxis="on",
                    title="Price Distribution per Product - Histogram", chartW=12, chartH=16)
        print("working on products category treemap viz")
        # Plot 3
        dfCategTreeMap = query_postgresql(query_category_treemap)
        plot_charts(df=dfCategTreeMap, type="treemap", subplot_rows=subplot_rows, subplot_cols=subplot_cols, chart_pos=4, hasAxis="off",
                    title="Top 10 Categories in # of Products", chartW=12, chartH=16)
        print("working on top categories bar viz")
        # Plot 4
        dfCategBar = query_postgresql(query_category_bar)
        plot_charts(df=dfCategBar, type="bar", subplot_rows=subplot_rows, subplot_cols=subplot_cols, chart_pos=3, hasAxis="on",
                    title="Top 5 Categories by # of Products", chartW=12, chartH=16)
        plt.savefig(f"{absolutePathToApp}/output_report/plots.pdf")

        # Send the charts in a pdf by Email
        for email in email_list:
            send_pdf = sendpdf(sender_email,
                               email,
                               sender_password,
                               "Amazon Products - Data Report",
                               "Take a look at the attached data report",
                               "plots",
                               f"{absolutePathToApp}/output_report")
            send_pdf.email_send()
        print("plots were sucessfully sent to pdf")
    except Exception as e:
        print("log: app failed to save charts to pdf, error is: ", e)


# Do work
# generate_viz_report(email_list=["mateusleao81@gmail.com"], sender_email = "mateusilvaleao@gmail.com", sender_password = "$!banJ0210196!$")
# reshma.wadhwa@justeattakeaway.com
