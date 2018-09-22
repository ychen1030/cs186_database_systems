#!/bin/bash

HW_FILE=${1-hw1.sql}
DATASET_NAME=${2-baseball}
: ${EXPECTED_OUTPUT:=expected_output}
: ${QUERIES:=all}

rm -rf your_output 2>/dev/null
mkdir your_output 2>/dev/null

rm -rf diffs 2>/dev/null
mkdir diffs 2>/dev/null

psql -E -d "$DATASET_NAME" < "$HW_FILE" >&2

pass=true

test_query() {
    local query=$1
    local test_name=$2
    local your_output_file="your_output/${test_name}.txt"
    local expected_output_file="${EXPECTED_OUTPUT}/${test_name}.txt"
    local expected_output_alternative="${EXPECTED_OUTPUT}/q4v_alt.txt"
    local diff_file="diffs/${test_name}.txt"

    if [[ ! ($QUERIES == "all" || $QUERIES =~ (^|[[:space:]])"$test_name"([[:space:]]|$) ) ]]; then
        return
    fi

    if ! (psql -c "$query" -d "$DATASET_NAME" &>"$your_output_file" ); then
        pass=false
        echo "ERROR $test_name! See $your_output_file"
    else
        diff "$your_output_file" "$expected_output_file" > "$diff_file"
        if [ $? -ne 0 ]; then
            if [[("$test_name" == "q4v")]]; then
		diff "$your_output_file" "$expected_output_alternative" > "$diff_file"
	        if [ $? -ne 0 ]; then
		    pass=false
	            echo "ERROR $test_name! See $diff_file"
		else
		    echo "PASS $test_name"
	        fi
	    else
		pass=false
	        echo "ERROR $test_name! See $diff_file"
	    fi
            #pass=false
            #echo "ERROR $test_name! See $diff_file"
        else
            echo "PASS $test_name"
        fi
    fi
}

test_query "SELECT * FROM q0;" q0
test_query "SELECT * FROM q1i ORDER BY namefirst, namelast, birthyear;" q1i
test_query "SELECT * FROM q1ii ORDER BY namefirst, namelast, birthyear;" q1ii
test_query "SELECT * FROM q1iii;" q1iii
test_query "SELECT * FROM q1iv;" q1iv
test_query "SELECT * FROM q2i;" q2i
test_query "SELECT * FROM q2ii;" q2ii
test_query "SELECT * FROM q2iii;" q2iii
test_query "SELECT * FROM q3i;" q3i
test_query "SELECT * FROM q3ii;" q3ii
test_query "SELECT * FROM q3iii ORDER BY namefirst, namelast;" q3iii
test_query "SELECT * FROM q4i;" q4i
test_query "SELECT * FROM q4ii WHERE binid <> 9;" q4ii_bins_0_to_8
test_query "WITH max_salary AS (SELECT MAX(salary) AS salary FROM salaries)
    SELECT binid, low,
           CONCAT(CASE WHEN high >= salary THEN '' ELSE 'not ' END,
                  'at least ',
                  salary) AS high,
           count
    FROM q4ii, max_salary WHERE binid = 9;" q4ii_bin_9
test_query "SELECT * FROM q4iii;" q4iii
test_query "SELECT * FROM q4iv ORDER BY yearid, playerid;" q4iv
test_query "SELECT * FROM q4v order by team asc;" q4v

if $pass; then
    echo "SUCCESS: Your queries passed tests on this dataset."
    exit 0
else
    exit 1
fi

