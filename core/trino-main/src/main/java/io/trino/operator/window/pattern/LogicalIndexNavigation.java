/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator.window.pattern;

import io.trino.operator.window.matcher.ArrayView;

import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class LogicalIndexNavigation
{
    // a set of labels to navigate over:
    // LAST(A.price, 3) => this is a navigation over rows with label A, so labels = {A}
    // LAST(Union.price, 3) => this is a navigation over rows matching a union variable Union, so for SUBSET Union = (A, B, C), we have labels = {A, B, C}
    // LAST(price, 3) => this is a navigation over "universal pattern variable", which is effectively over all rows, no matter the assigned labels. In such case labels = {}
    private final Set<Integer> labels;
    private final boolean last;
    private final boolean running;
    private final int logicalOffset;
    private final int physicalOffset;

    public LogicalIndexNavigation(Set<Integer> labels, boolean last, boolean running, int logicalOffset, int physicalOffset)
    {
        this.labels = requireNonNull(labels, "labels is null");
        this.last = last;
        this.running = running;
        checkArgument(logicalOffset >= 0, "logical offset must be >= 0, actual: ", logicalOffset);
        this.logicalOffset = logicalOffset;
        this.physicalOffset = physicalOffset;
    }

    /**
     * This method is used for resolving positions during pattern matching. The `newLabel` is the label currently being matched.
     * For the purpose of match evaluation, the new label is considered as matched in the next position after all `matchedLabels`.
     *
     * @return position within partition, or -1 if matching position was not found
     */
    public int resolvePosition(ArrayView matchedLabels, int newLabel, int searchStart, int searchEnd, int patternStart)
    {
        int relativePosition;
        if (last) {
            relativePosition = findLastAndBackwards(matchedLabels, newLabel);
        }
        else {
            relativePosition = findFirstAndForward(matchedLabels, newLabel);
        }
        return adjustPosition(relativePosition, patternStart, searchStart, searchEnd);
    }

    // LAST(A.price, 3): find the last occurrence of label "A" and go 3 occurrences backwards
    private int findLastAndBackwards(ArrayView matchedLabels, int newLabel)
    {
        int position = matchedLabels.length();
        int found = 0;
        // the new label is considered as matched for the purpose of navigation
        if (labels.isEmpty() || labels.contains(newLabel)) { // empty label denotes "universal row pattern variable", which always matches
            found++;
        }
        while (found <= logicalOffset && position > 0) {
            position--;
            if (labels.isEmpty() || labels.contains(matchedLabels.get(position))) {
                found++;
            }
        }
        if (found == logicalOffset + 1) {
            return position;
        }
        return -1;
    }

    // FIRST(A.price, 3): find the first occurrence of label "A" and go 3 occurrences forward
    private int findFirstAndForward(ArrayView matchedLabels, int newLabel)
    {
        int position = -1;
        int found = 0;
        while (found <= logicalOffset && position < matchedLabels.length() - 1) {
            position++;
            if (labels.isEmpty() || labels.contains(matchedLabels.get(position))) {
                found++;
            }
        }
        // the new label is considered as matched for the purpose of navigation
        if (found <= logicalOffset) {
            position++;
            if (labels.isEmpty() || labels.contains(newLabel)) {
                found++;
            }
        }
        if (found == logicalOffset + 1) {
            return position;
        }
        return -1;
    }

    // adjust position by patternStart to reflect position within partition
    // adjust position by physical offset: skip a certain number of rows, regardless of labels
    // check if the new position is within partition bound by: partitionStart - inclusive, partitionEnd - exclusive
    private int adjustPosition(int relativePosition, int patternStart, int searchStart, int searchEnd)
    {
        if (relativePosition == -1) {
            return -1;
        }
        int start = relativePosition + patternStart;
        int target = start + physicalOffset;
        if (target < searchStart || target >= searchEnd) {
            return -1;
        }
        return target;
    }

    /**
     * This method is used when computing row pattern measures and SKIP TO position after finding a match. Array of matched labels is complete.
     * Search is limited up to the current row in case of running semantics and to the entire match in case of final semantics.
     *
     * @return position within partition, or -1 if matching position was not found
     */
    public int resolvePosition(int currentRow, ArrayView matchedLabels, int searchStart, int searchEnd, int patternStart)
    {
        checkArgument(currentRow >= patternStart && currentRow < patternStart + matchedLabels.length(), "current row is out of bounds of the match");

        int relativePosition;
        if (last) {
            int start;
            if (running) {
                start = currentRow - patternStart;
            }
            else {
                start = matchedLabels.length() - 1;
            }
            relativePosition = findLastAndBackwards(start, matchedLabels);
        }
        else {
            relativePosition = findFirstAndForward(matchedLabels);
        }
        return adjustPosition(relativePosition, patternStart, searchStart, searchEnd);
    }

    // LAST(A.price, 3): find the last occurrence of label "A" and go 3 occurrences backwards
    private int findLastAndBackwards(int searchStart, ArrayView matchedLabels)
    {
        int position = searchStart + 1;
        int found = 0;
        while (found <= logicalOffset && position > 0) {
            position--;
            if (labels.isEmpty() || labels.contains(matchedLabels.get(position))) {
                found++;
            }
        }
        if (found == logicalOffset + 1) {
            return position;
        }
        return -1;
    }

    // FIRST(A.price, 3): find the first occurrence of label "A" and go 3 occurrences forward
    private int findFirstAndForward(ArrayView matchedLabels)
    {
        int position = -1;
        int found = 0;
        while (found <= logicalOffset && position < matchedLabels.length() - 1) {
            position++;
            if (labels.isEmpty() || labels.contains(matchedLabels.get(position))) {
                found++;
            }
        }
        if (found == logicalOffset + 1) {
            return position;
        }
        return -1;
    }
}
