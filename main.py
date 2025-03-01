# orice proiect trebuie sa aiba un config.json, requairements.txt, README.md

import books_db_action as db
from init_config import config
import gradio as gr
import pandas as pd
import os
from pydub.utils import db_to_float


def get_book(user_input):

    if user_input:
        query = f"""select b."name", b.number_of_sales, b.reviews from authors a
                    join book b on a.author_id = b.author_id
                    where a."name" = '{user_input}'
                    order by b.number_of_sales desc;
                    """
        response = db.get_data(query, database_config)
        df = pd.DataFrame(response)
        if df.empty:
           return df
        else:
           raise Exception("Author was not found...")
    else:
        print("You need to insert an author before pressing the button")
        raise Exception("You need to insert an author before pressing the button")


def add_book(name, sales, review, author):
    if name and sales and review and author:
        if not sales.isnumeric():
            raise Exception("Sales parameters must be number!!!")
        if not review.isnumeric():
            raise Exception("Review parameter must be a number!!!")
        if author.isnumeric():
            raise Exception("Author parameter must be a string!!!")

# sql_query = "insert into books(\"name\", number_of_sales, reviews, author_id) values ('harry potter 700', 20000, 8, 1);"
        author_name = db.get_data(f"""select author_id from authors a where "name" = '{author}' limit 1;""", database_config)
        if len(author_name) > 0:
            author_id =author_name[0]['author_id']
            db.insert_row(f"""insert into books(\"name\", number_of_sales, reviews, author_id) values ('{name}', {int(sales)}, {int(review)}, {int(author_id)});""", database_config)
        else:
            raise Exception("Author was not found in database")
        print(author_name)



    else:
        raise Exception("All values are mandatory")
    print(author_name)


def delete_book(name):
    if name:
        db.delete_row(name, database_config)
    else:
        raise Exception("Book name is mandatory for delete")


def table_change(table, author_name):
    # TODO to update database on books, we need to compare database framework with actual table
    print(table)
    original_table = get_book(author_name)
    if not original_table.eguals(table):
        diff = original_table.compare(table)

    print(original_table)


def start_gui_app():

    with (gr.Blocks() as app):
        with gr.Row():
           with gr.Column(scale=1):
             author_input = gr.Textbox(label="Write an author")
           with gr.Column(scale=1):
             get_book_button = gr.Button("Show Books")

        with gr.Row():
            response_table = gr.Dataframe(label="Results", interactive=True)
            response_table.change(fn=table_change, inputs=[response_table, author_input])
            get_book_button.click(fn=get_book, inputs=author_input, outputs=response_table)

        with gr.Row():
                with gr.Column(scale=1):
                    new_book = gr.Textbox(label="New Book")
                with gr.Column(scale=1):
                    number_of_sales = gr.Textbox(label="Sales")
                with gr.Column(scale=1):
                    review = gr.Textbox(label="Review")
                with gr.Column(scale=1):
                    author = gr.Textbox(label="Author Name")

        with gr.Row():
            add_book_btn = gr.Button("Add Book")
            add_book_btn.click(fn=add_book, inputs=[new_book, number_of_sales, review, author])

        with gr.Row():
            with gr.Column(scale=1):
                delete_book_input = gr.Textbox(label="Delete book")
                delete_book_btn = gr.Button("Delete")
                delete_book_btn.click(fn=delete_book, inputs=delete_book_input)

        app.launch(inbrowser=True, show_error=True)


if __name__ == '__main__':
    database_config = config.get("database_config")
    database_config['password'] = os.environ['postgres']
    start_gui_app()

