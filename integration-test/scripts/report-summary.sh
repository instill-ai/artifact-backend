#!/bin/bash
# Integration Test Summary Reporter
# Parses k6 test output and generates a clean summary report

# ANSI color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;36m'
NC='\033[0m' # No Color

# Parse the log file for test results
parse_results() {
    local log_file="$1"
    local total_passed=0
    local total_checks=0
    local all_passed=true

    echo ""
    echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║          INTEGRATION TEST SUMMARY REPORT                   ║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""

    # Detect log format: sequential (has "Running integration-test/") or parallel (has test filename prefix on each line)
    local is_parallel_format=false
    if grep -q "^integration-test/.*checks_total" "$log_file" 2>/dev/null; then
        is_parallel_format=true
    fi

    # Extract test results for each file (order matches Makefile batches)
    for test_file in \
        "rest.js" \
        "rest-object-storage.js" \
        "rest-hash-based-ids.js" \
        "rest-file-type.js" \
        "rest-db.js" \
        "rest-ai-client.js" \
        "rest-kb-e2e-file-process.js" \
        "rest-file-reprocess.js" \
        "rest-kb-delete.js" \
        "grpc.js" \
        "grpc-kb-update.js" \
        "grpc-system-config-update.js" \
        "grpc-system-admin.js"; do

        local total=""
        local succeeded=""

        if [ "$is_parallel_format" = true ]; then
            # Parallel format: Each line prefixed with "integration-test/test.js\t"
            local total_line=$(grep -a "^integration-test/${test_file}[[:space:]]" "$log_file" | grep -a "checks_total" | tail -1)
            local succeeded_line=$(grep -a "^integration-test/${test_file}[[:space:]]" "$log_file" | grep -a "checks_succeeded" | tail -1)

            if [ -n "$total_line" ]; then
                total=$(echo "$total_line" | awk '{for(i=1;i<=NF;i++) if($i ~ /^[0-9]+$/) {print $i; exit}}')
            fi
            if [ -n "$succeeded_line" ]; then
                succeeded=$(echo "$succeeded_line" | awk '{for(i=1;i<=NF;i++) if($i=="out") print $(i-1)}')
            fi
        else
            # Sequential format: Test sections marked by "Running integration-test/test.js..."
            local start_line=$(grep -n "Running integration-test/${test_file}" "$log_file" | tail -1 | cut -d: -f1)

            if [ -n "$start_line" ]; then
                local next_test_line=$(awk -v start="$start_line" 'NR > start && /^Running integration-test\// {print NR; exit}' "$log_file")
                if [ -z "$next_test_line" ]; then
                    next_test_line=$(wc -l < "$log_file")
                fi

                local test_section=$(sed -n "${start_line},${next_test_line}p" "$log_file")
                local total_line=$(echo "$test_section" | grep -a "checks_total" | tail -1)
                local succeeded_line=$(echo "$test_section" | grep -a "checks_succeeded" | tail -1)

                if [ -n "$total_line" ]; then
                    total=$(echo "$total_line" | awk '{for(i=1;i<=NF;i++) if($i ~ /^[0-9]+$/) {print $i; exit}}')
                fi
                if [ -n "$succeeded_line" ]; then
                    succeeded=$(echo "$succeeded_line" | awk '{for(i=1;i<=NF;i++) if($i=="out") print $(i-1)}')
                fi
            fi
        fi

        # Report results if we found them
        if [ -n "$succeeded" ] && [ -n "$total" ]; then
            total_passed=$((total_passed + succeeded))
            total_checks=$((total_checks + total))

            # Format test name (30 chars wide)
            local test_name=$(printf "%-30s" "$test_file")

            # Calculate percentage
            local percentage=$((succeeded * 100 / total))

            # Determine status icon
            if [ "$succeeded" -eq "$total" ]; then
                echo -e "${GREEN}✅${NC} ${BLUE}${test_name} ${succeeded}/${total}   (${percentage}%)${NC}"
            else
                echo -e "${RED}❌${NC} ${BLUE}${test_name} ${succeeded}/${total}   (${percentage}%)${NC}"
                all_passed=false
            fi
        fi
    done

    echo -e "${BLUE}─────────────────────────────────────────────────────────────${NC}"

    # Guard against division by zero
    if [ "$total_checks" -eq 0 ]; then
        echo -e "${RED}❌ ERROR:${NC} ${BLUE}No test results found in log file${NC}"
        echo ""
        return 1
    fi

    # Calculate total percentage
    local total_percentage=$((total_passed * 100 / total_checks))

    # Print total summary
    if $all_passed; then
        echo -e "${GREEN}✅ TOTAL:${NC}                      ${BLUE}${total_passed}/${total_checks} (${total_percentage}%)${NC}"
        echo ""
        echo -e "${GREEN}🎉 ALL TESTS PASSED!${NC}"
    else
        echo -e "${RED}❌ TOTAL:${NC}                      ${BLUE}${total_passed}/${total_checks} (${total_percentage}%)${NC}"
        echo ""
        echo -e "${RED}⚠️  SOME TESTS FAILED${NC}"
    fi

    echo ""

    # Return exit code based on results
    if $all_passed; then
        return 0
    else
        return 1
    fi
}

# Main execution
if [ -z "$1" ]; then
    echo -e "${BLUE}Usage: $0 <log_file>${NC}"
    exit 1
fi

if [ ! -f "$1" ]; then
    echo -e "${RED}Error:${NC} ${BLUE}Log file not found: $1${NC}"
    exit 1
fi

parse_results "$1"
exit $?
