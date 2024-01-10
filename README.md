# topsort

## Problem Statement
Imagine a file in the following fixed format:
`<url><white_space><long value>` e.g.
```
http://api.tech.com/item/121345 9
http://api.tech.com/item/122345 350
http://api.tech.com/item/123345 25
http://api.tech.com/item/124345 231
http://api.tech.com/item/125345 111
...
```
Write a program that reads from 'stdin' the absolute path of a file expected
to be in this format and outputs a list of the urls associated with the 10
largest values in the right-most column. For example, given the input data
above if the question were to output the 2 largest values the output would
be:
```
http://api.tech.com/item/122345
http://api.tech.com/item/124345
```
Your solution should take into account extremely large files.

## Test data

Test data can be generated using the Python script `generate_data.py`, which takes the number of records as an argument and outputs the results to the standard output. Here is an example of how to use it:
```
python generate_data.py 1000000 > data.csv
```

In my scenarios, I used three files:

| name  | # of records | size  |
| ----- | ------------ | ----- |
| data  | 1M           | 40MB  |
| data2 | 10M          | 400MB |
| data3 | 100M         | 4GB   |

### Solution

The most naive approach could be a bash script like this: `sort -k2,2rn <file> | head`. Sorting seems to work well even for medium-sized files, but the problem becomes evident with larger files.

```
$  time (sort -k2,2rn data.csv | head)
real    0m0.289s
user    0m0.843s
sys     0m0.046s

$  time (sort -k2,2rn data2.csv | head)
real    0m2.717s
user    0m12.530s
sys     0m0.358s

$  time (sort -k2,2rn data3.csv | head)
real    1m43.498s
user    4m57.406s
sys     0m6.140s
```

A better solution is to read a file in parallel and merge the results. You can find the solution in `main.go`. It can be executed using:
```
go run . <file-path>
```
Replace <file-path> with the path to the file containing link records.

Short description:
1. The program reads the specified file in chunks.
  * It divides the file into chunks and processes each chunk concurrently using goroutines.
2. The program merges partial results obtained from different chunks.
  * The merging process is designed to efficiently identify and keep the top results based on link size.

Some features:
* The program utilizes goroutines to parallelize the reading, processing, and merging of data.
* It uses semaphores (`readings` and `merges`) to control the concurrent execution of reading and merging operations.

Results from running the program on the same input data:

```
$ time go run . data.csv
real    0m1.796s
user    0m0.000s
sys     0m0.218s

$ time go run . data2.csv
real    0m2.663s
user    0m0.015s
sys     0m0.171s

$ time go run . data3.csv
real    0m14.217s
user    0m0.000s
sys     0m0.155s
```

The solution uses only standard libraries and could be simplified with the help of external libraries such as `errgroup` or `rxgo`.


### Testing
The main testing approach is to compare the results of the programs with the results of the `sort` command on various test data.

## Alternative solution
Here we can observe a problem that can be solved using the Map-Reduce approach, and Apache Spark should be a perfect fit in this case. It can handle significant loads, and the solution would only require a few lines of code.
