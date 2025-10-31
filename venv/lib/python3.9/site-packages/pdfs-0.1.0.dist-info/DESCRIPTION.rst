# pdfs
Simple Bibliography Database

## Introduction

This tool manages article PDFs.  This tool is based on two working principles.
First, DOIs are central. Only documents with a DOI can be stored in the
database. All metadata is acquired through the [Crossref](https://crossref.org)
[API](https://github.com/CrossRef/rest-api-doc) through DOIs scraped from the
PDF or entered by the user. Second, document repositories are local to a
directory hierarchy. For example, if I have a project based in ~/projects/foo,
I can create a document repository by 

    cd ~/projects/foo
    pdfs init

This creates a new direcory in ~/projects/foo called articles. All PDFs
imported into the database will be copied here. Running `pdfs` in
~/projects/foo, or any directory below it will use the repository at
~/projects/foo/articles.

## Commands

    usage: pdfs command ...

| Command | Description                                |
|---------|--------------------------------------------|
| add     | Import new PDF into repository             |
| aux2bib | Read LaTeX .aux file and dump a .bib file  |
| bibtex  | Dump bibtex for keys                       |
| edit    | Edit bibtex, metadata and file attachments |
| import  | Import entries from other database         |
| info    | Print information about current repository |
| init    | Initialize new document repository         |
| list    | List all items in database                 |
| search  | Search full text of PDF                    |
| view    | View article PDF and attachements          |
| watch   | Watch a directory for new pdf files to add |
| www     | Spin up http server                        |


## Command line completion

Command line autocomplete support via 
[argcomplete](https://argcomplete.readthedocs.io). 

To start quickly with zsh:

    autoload bashcompinit
    bashcompinit
    eval "$(register-python-argcomplete pdfs)"

