## Introduction
This project finds the number of times the most-visited page was visited each hour in the dataset. It will ignore the fron page `title == "Main_Page"` and "special" `title.startsWith("Special:")`. 

Spark RDD is used. 

## How to Run
Import this project as an Maven project. Run the `main` method in the `WikipediaPopular.java`.

## Steps
1. Read the input file(s) in as lines (as in the word count).
2. Break each line up into a tuple of five things (by splitting around spaces). This would be a good time
to convert the view count to an integer (.map())
3. Remove the records we don't want to consider (.filter())
4. Create an RDD of key-value pairs (.map())
5. Reduce to find the max value for each key (.reduceByKey())
6. Sort so the records are sorted by key (.sortBy())
7. Save as text output (see hints below).

## Low-core Processor VS High-core Processor Perofrmance

* High Core

    `Total time with 8 cores CPU is 38985 ms`
    
* Low Core

    `Total time with 4 cores CPU is 69783 ms`
