use postgres::{Client, Error, NoTls};
use std::collections::HashMap;

// Structure for Author and Book
#[derive(Debug)]
struct Author {
    _id: i32,
    name: String,
    country: String,
}

#[derive(Debug)]
struct Book {
    _id: i32,
    title: String,
    author_id: i32,
}

#[derive(Debug)]
struct Nation {
    nationality: String,
    count: i64,
}

fn main() -> Result<(), Error> {
    // Connection with the database (Postgres)
    let mut client = Client::connect("postgresql://Abhinav:Abhinav123@localhost/Rust_API", NoTls)?;

    // Create the 'author' table
    client.batch_execute(
        "
        CREATE TABLE IF NOT EXISTS author (
            id              SERIAL PRIMARY KEY,
            name            VARCHAR NOT NULL,
            country         VARCHAR NOT NULL
        )
    ",
    )?;

    // Create the 'book' table
    client.batch_execute(
        "
        CREATE TABLE IF NOT EXISTS book  (
            id              SERIAL PRIMARY KEY,
            title           VARCHAR NOT NULL,
            author_id       INTEGER NOT NULL REFERENCES author
        )
    ",
    )?;

    // Insert into 'author' table
    let mut authors = HashMap::new();
    authors.insert(String::from("Jidion"), "Nigeria");
    authors.insert(String::from("Sam"), "United Kingdom");
    authors.insert(String::from("Tina"), "India");

    for (key, value) in &authors {
        let author = Author {
            _id: 0,
            name: key.to_string(),
            country: value.to_string(),
        };

        client.execute(
            "INSERT INTO author (name, country) VALUES ($1, $2)",
            &[&author.name, &author.country],
        )?;
    }

    // Insert into 'book' table
    let mut books = HashMap::new();
    books.insert(String::from("Idiomatic Rust"), 1);
    books.insert(String::from("Lets lean Go with fun!"), 2);
    books.insert(String::from("Who told you JS sucks?"), 3);

    for (key, value) in &books {
        let book = Book {
            _id: 0,
            title: key.to_string(),
            author_id: *value,
        };

        client.execute(
            "INSERT INTO book (title, author_id) VALUES ($1, $2)",
            &[&book.title, &book.author_id],
        )?;
    }

    // Look up
    let authors = lookup_author(&mut client)?;
    let books = lookup_books(&mut client)?;

    for author in &authors {
        println!("Author: {:?}", author);
    }

    for book in &books {
        println!("Book: {:?}", book);
    }

    // Aggrigate data
    for row in client.query(
        "SELECT nationality, COUNT(nationality) AS count
        FROM artists GROUP BY nationality ORDER BY count DESC",
        &[],
    )? {
        let (nationality, count): (Option<String>, Option<i64>) = (row.get(0), row.get(1));

        if nationality.is_some() && count.is_some() {
            let nation = Nation {
                nationality: nationality.unwrap(),
                count: count.unwrap(),
            };

            println!("{} {}", nation.nationality, nation.count);
        }
    }

    Ok(())
}

// Helper function
// Lookup 'author' table data
fn lookup_author(client: &mut Client) -> Result<Vec<Author>, Error> {
    let mut authors = Vec::new();
    for row in client.query("SELECT id, name, country FROM author", &[])? {
        let author = Author {
            _id: row.get(0),
            name: row.get(1),
            country: row.get(2),
        };
        authors.push(author);
    }
    Ok(authors)
}

// Lookup 'book' table data
fn lookup_books(client: &mut Client) -> Result<Vec<Book>, Error> {
    let mut books = Vec::new();
    for row in client.query("SELECT id, title, author_id FROM book", &[])? {
        let book = Book {
            _id: row.get(0),
            title: row.get(1),
            author_id: row.get(2),
        };
        books.push(book);
    }
    Ok(books)
}
