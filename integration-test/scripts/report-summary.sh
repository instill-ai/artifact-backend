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

    # Extract test results for each file
    for test_file in \
        "grpc.js" \
        "rest.js" \
        "rest-file-type.js" \
        "rest-object-storage.js" \
        "rest-db.js" \
        "rest-ai-client.js" \
        "rest-kb-e2e-file-process.js" \
        "rest-file-reprocess.js" \
        "rest-kb-delete.js" \
        "grpc-system-config-update.js" \
        "grpc-kb-update.js" \
        "grpc-system-admin.js"; do

        # Check if this test file had any errors (timeout, execution failure, etc.)
        # Exclude warnings and informational error logs (level=warning, [POLL] logs)
        # Use word boundary to ensure exact test file match
        # Note: All actual failures now have explicit check() calls, so this is just a safety net
        local has_error=$(grep -a "^integration-test/${test_file}[[:space:]]" "$log_file" | grep -a -E "timed out|This job failed" | grep -a -v -E "level=warning|\[POLL\]" | head -1)

        # Find the line with checks_total for this specific test file
        # Format: "integration-test/test.js    checks_total.......: 20      0.659725/s"
        local total_line=$(grep -a "integration-test/${test_file}" "$log_file" | \
            grep -a "checks_total" | tail -1)

        if [ -n "$total_line" ]; then
            # Extract total count (3rd field in the line)
            local total=$(echo "$total_line" | awk '{print $3}')

            # Find the checks_succeeded line for this test
            local succeeded_line=$(grep -a "integration-test/${test_file}" "$log_file" | \
                grep -a "checks_succeeded" | tail -1)

            if [ -n "$succeeded_line" ]; then
                # Format: "integration-test/test.js    checks_succeeded...: 100.00% 20 out of 20"
                # Extract the first number after the percentage (field 4)
                local succeeded=$(echo "$succeeded_line" | awk '{print $4}')

                if [ -n "$succeeded" ] && [ -n "$total" ]; then
                    total_passed=$((total_passed + succeeded))
                    total_checks=$((total_checks + total))

                    # Format test name (30 chars wide)
                    local test_name=$(printf "%-30s" "$test_file")

                    # Calculate percentage
                    local percentage=$((succeeded * 100 / total))

                    # Determine status icon
                    # Mark as failed if there are errors, even if checks passed
                    if [ -n "$has_error" ]; then
                        echo -e "${RED}❌${NC} ${BLUE}${test_name} ${succeeded}/${total}   (${percentage}%)${NC} ${RED}[ERROR/TIMEOUT]${NC}"
                        all_passed=false
                    elif [ "$succeeded" -eq "$total" ]; then
                        echo -e "${GREEN}✅${NC} ${BLUE}${test_name} ${succeeded}/${total}   (${percentage}%)${NC}"
                    else
                        echo -e "${RED}❌${NC} ${BLUE}${test_name} ${succeeded}/${total}   (${percentage}%)${NC}"
                        all_passed=false
                    fi
                fi
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
