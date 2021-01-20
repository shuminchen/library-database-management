# You should write your Python program in this file. Currently, it contains
# a skeleton of the methods you will need to write.

import csv
import os
import sqlite3


# in Python, we specify functions using "def" -- this would be equiv to Java
# `public void load_data()`. Note that Python doesn't specify return types.
def load_data():
    # This function should:
    # 1) create a new database file called "library.db"
    # 2) create appropriate tables
    # 3) read the data.csv file and insert data into your database

    # first, we will check to see if library.db already exists.
    # if it does, we will delete it.
    if os.path.exists("library.db"):
        os.remove("library.db")

    # create a database connection.
    conn = sqlite3.connect("library.db")
    # create a cursor (this is like a single session)
    curr = conn.cursor()
    # send a pragma command to tell SQLite to check foreign key
    curr.execute("PRAGMA foreign_keys = ON;")

    # create a table called 'author_list'
    curr.execute("CREATE TABLE author_list ( author_name TEXT PRIMARY KEY, author_birth_year INTEGER   )")

    # create a table called 'book_list'
    curr.execute("CREATE TABLE book_list (book_name TEXT, book_barcode TEXT "
                 "PRIMARY KEY, publisher_name TEXT, book_year INTEGER, author TEXT)")

    # create a table called 'patron_list'
    curr.execute("CREATE TABLE patron_list (patron_name TEXT, patron_card TEXT PRIMARY KEY, phone_number TEXT, join_year REAL)")

    # create a table called 'publisher_list'
    curr.execute("CREATE TABLE publisher_list (publisher_name TEXT, publisher_phone_number TEXT)")


    # create a table called 'patron_borrow_list'
    curr.execute("CREATE TABLE patron_borrow_list (patron_card TEXT REFERENCES patron_list(patron_card), checkout_date REAL, book_barcode TEXT, due_date REAL, returned INTEGER)")
    conn.commit()
    # read the data in the CSV file into the table 'borrow_list'.
    with open("data.csv") as f:
        reader = csv.reader(f)
        next(reader) # throw out the header row

        for row in reader:
            # import data into the patron_list table
            curr.execute("INSERT OR IGNORE INTO patron_list VALUES (?, ?, ?, ? )", (row[0],  row[1], row[3], row[2]))

            # import data into the patron_borrow_list table
            curr.execute("INSERT OR IGNORE INTO patron_borrow_list VALUES (?, ?, ?, ?, ? )",
                         (row[1],  row[11], row[4],  row[12],  row[13]))
            # import data into the book_list table
            curr.execute("INSERT OR IGNORE INTO book_list VALUES (?, ?, ?, ?,? )", (row[5], row[4], row[9], row[6], row[7]))

            # import data into the publisher_list table
            curr.execute("INSERT OR IGNORE INTO publisher_list VALUES (?, ?)", (row[9], row[10]))

            # import data into the author_list table
            curr.execute("INSERT OR IGNORE INTO author_list VALUES (?, ?)", (row[7], row[8]))
            print (row)
            conn.commit()# save the table
    # close the BD connection
    conn.close()




def overdue_books(date_str):
    # This function should take in a string like YYYY-MM-DD and print out
    # a report of all books that are overdue as of that date, and the
    # patrons who still have them.

    # create a database connection.
    conn = sqlite3.connect("library.db")

    # create a cursor (this is like a single session)
    curr = conn.cursor()

    curr.execute("PRAGMA foreign_keys = ON;")

    # read data from the database
    curr.execute("SELECT book_name, patron_name FROM patron_borrow_list, patron_list, book_list "
                 "WHERE due_date <julianday(?) AND Returned = 0 "
                 "AND patron_borrow_list.patron_card = patron_list.patron_card "
                 "AND patron_borrow_list.book_barcode = book_list.book_barcode",
                 (date_str,))
    for row in curr:
        # each time through the loop, row[0] will be the first column
        # of the result, and row[1] will be the second.
        print("Book: " + row[0] + "; Patron: " + row[1])

    conn.close() # close the DB connection when we are done


def most_popular_books():
    # This function should print out a report of which books are the
    # most popular (checked out most frequently). The library cares about
    # the books themselves, not who published them.

    # create a database connection.
    conn = sqlite3.connect("library.db")

    # create a cursor (this is like a single session)
    curr = conn.cursor()

    curr.execute("PRAGMA foreign_keys = ON;")

    # read data from the database
    curr.execute("SELECT book_name, patron_borrow_list.book_barcode, COUNT(book_name) "
                 "FROM patron_borrow_list, book_list "
                 "WHERE patron_borrow_list.book_barcode = book_list.book_barcode "
                 "GROUP BY book_name "
                 "ORDER BY COUNT(book_name) DESC "
                 "LIMIT 10")

    for row in curr:
        # each time through the loop, row[0] will be the first column
        # of the result, and row[1] will be the second.
        print(row[0] + ', ' + row[1])

    conn.close() # close the DB connection when we are done



def note_return(patron_card, book_barcode):
    # This function should update the database to indicate that the patron
    # with the passed card number has returned the book with the given
    # barcode. This function should print out an error if that patron didn't
    # have the book currently checked out.

    # create a database connection.
    conn = sqlite3.connect("library.db")

    # create a cursor (this is like a single session)
    curr = conn.cursor()

    curr.execute("PRAGMA foreign_keys = ON;")

    # check if the patron have the book currently checked out
    curr.execute("SELECT * FROM patron_borrow_list "
                 "WHERE returned = 0 "
                 "AND book_barcode = (?) "
                 "AND patron_card = (?)  ",
                 (book_barcode, patron_card))

    result = curr.fetchone()
    if result == None:
        print("Error: The patron did not borrow or the book has been returned")
    else:
        # read data from the database
        curr.execute("UPDATE patron_borrow_list SET returned = 1 WHERE book_barcode = (?) AND patron_card = (?)  ",
                     (book_barcode, patron_card))
        # save updates
        conn.commit()
        print("Book returned")

    # close the DB connection when we are done
    conn.close()



def note_checkout(patron_card, book_barcode, checkout_date):
    # This function should update the database to indicate that a patron
    # has checked out a book on the passed date. The due date of the book
    # should be 7 days after the checkout date. This function should print
    # out an error if the book is currently checked out.

    # create a database connection.
    conn = sqlite3.connect("library.db")

    # create a cursor (this is like a single session)
    curr = conn.cursor()

    curr.execute("PRAGMA foreign_keys = ON;")

    curr.execute("SELECT * FROM patron_list WHERE patron_card = (?)", (patron_card,))
    res1 = curr.fetchone()
    if res1 == None:
        print("Patron not found.")
    else:
        curr.execute("SELECT * FROM book_list WHERE book_barcode = (?)", (book_barcode,))
        res2 = curr.fetchone()
        if res2 == None:
            print("book not found in the library.")
        else:
            curr.execute("SELECT * FROM patron_borrow_list "
                         "WHERE patron_card = (?) "
                         "AND book_barcode = (?) "
                         "AND returned = 0",
                         (patron_card, book_barcode))
            res3 = curr.fetchone()
            if res3 == None:
                curr.execute("INSERT INTO patron_borrow_list VALUES (?, julianday(?), ?, julianday(?)+7, 0 )",
                             (patron_card, checkout_date, book_barcode, checkout_date))

                # save updates
                conn.commit()
                print("successfully check out")
            else:
                print("This patron already borrowed this book.")

    # close the DB connection when we are done
    conn.close()


def replacement_report(book_barcode):
    # This function will be used by the library when a book has been lost
    # by a patron. It should print out: the publisher and publisher's contact
    # information, the patron who had checked out the book, and that patron's
    # phone number.

    # create a database connection.
    conn = sqlite3.connect("library.db")

    # create a cursor (this is like a single session)
    curr = conn.cursor()

    curr.execute("PRAGMA foreign_keys = ON;")

    res1 = curr.execute("SELECT * FROM book_list WHERE book_barcode = (?)", (book_barcode,)) # check if the lib had this book
    if res1 == None:
        print ("The library don't have this book")
    else:
        res2 = curr.execute("SELECT * FROM patron_borrow_list "
                            "WHERE book_barcode = (?) "
                            "AND returned = 0 ", (book_barcode,))
        if res2 == None:
            print("This book has already been returned")
        else:
            curr.execute("SELECT DISTINCT publisher_list.publisher_name, publisher_phone_number  FROM publisher_list, book_list "
                         "WHERE book_list.publisher_name = publisher_list.publisher_name "
                         "AND book_barcode = (?)  ", (book_barcode,))
            publisher_info = curr.fetchall()

            curr.execute("SELECT DISTINCT patron_list.patron_name, patron_list.phone_number FROM patron_borrow_list,  patron_list  "
                         "WHERE patron_list.patron_card = patron_borrow_list.patron_card "
                         "AND patron_borrow_list.book_barcode = (?)  ", (book_barcode,))
            patron_info = curr.fetchall()

            print("Publishers' info:")
            for row in publisher_info:
                print("Publisher: " + row[0])
                print("Publisher's phone number: " + row[1])
            print()
            print("patrons' info:")
            for row in patron_info:
                print("Patron: " + row[0])
                print("Patron's phone number: " + row[1])

    conn.close() # close the DB connection when we are done


def inventory():
    # This function should report the library's inventory, the books currently
    # available (not checked out).

    # create a database connection.
    conn = sqlite3.connect("library.db")

    # create a cursor (this is like a single session)
    curr = conn.cursor()
    # check constrains
    curr.execute("PRAGMA foreign_keys = ON;")

    curr.execute("SELECT DISTINCT book_name FROM book_list "
                 "WHERE book_list.book_barcode NOT IN ( "
                 "SELECT DISTINCT patron_borrow_list.book_barcode "
                 "FROM patron_borrow_list "
                 "WHERE patron_borrow_list.returned = 0)  "
                 "ORDER BY book_list.book_name ASC;")

    for row in curr:
        print(row)

    conn.close() # close the DB connection when we are done
#    pass # delete this when you write your code


# this is the entry point to a Python program, like `public static void main`
# in Java.
if __name__ == "__main__":
    while True:
        print("Hello! Welcome to the library system. What can I help you with today?")
        print("\t1) Load data")
        print("\t2) Overdue books")
        print("\t3) Popular books")
        print("\t4) Book return")
        print("\t5) Book checkout")
        print("\t6) Book replacement")
        print("\t7) Inventory")
        print("\t8) Quit")

        user_response = int(input("Select an option: "))

        if user_response == 1:
            load_data()
        elif user_response == 2:
            date = input("Date (YYYY-MM-DD): ")
            overdue_books(date)
        elif user_response == 3:
            most_popular_books()
        elif user_response == 4:
            patron = input("Patron card: ")
            book = input("Book barcode: ")
            note_return(patron, book)
        elif user_response == 5:
            patron = input("Patron card: ")
            book = input("Book barcode: ")
            chd = input("Checkout date (YYYY-MM-DD): ")
            note_checkout(patron, book, chd)
        elif user_response == 6:
            book = input("Book barcode: ")
            replacement_report(book)
        elif user_response == 7:
            inventory()
        elif user_response == 8:
            break
        else:
            print("Unrecognized option. Please try again.")
