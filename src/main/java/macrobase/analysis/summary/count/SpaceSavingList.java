package macrobase.analysis.summary.count;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpaceSavingList extends ApproximateCount {
    private static final Logger log = LoggerFactory.getLogger(SpaceSavingList.class);

    Map<Integer, CounterToken> digest = new HashMap<>();
    CounterGroup groupHead = null;
    private final int maxSize;
    private double totalCount;

    @Override
    public double getTotalCount() {
        return totalCount;
    }

    private class CounterGroup {
        private double differential;

        private CounterGroup prev;
        private CounterGroup next;
        private CounterToken tokenList;

        private CounterGroup(double differential,
                             CounterGroup prev,
                             CounterGroup next,
                             CounterToken counterToken) {
            this.differential = differential;
            this.prev = prev;
            this.next = next;
            this.tokenList = counterToken;
            counterToken.group = this;
        }

        public void removeIfEmpty() {
            if (tokenList != null) {
                return;
            }

            if (next != null) {
                next.prev = prev;
                next.differential += differential;
            }

            if (prev != null) {
                prev.next = next;
            } else {
                assert (groupHead == this);
                groupHead = next;
            }
        }

        public void addCounter(CounterToken t) {
            assert (tokenList != null);

            if (tokenList == null) {
                tokenList = t;
            } else {
                t.next = tokenList;
                t.next.prev = t;
                t.prev = null;

                tokenList = t;
            }

            t.group = this;
        }

        // returns replaced token
        public CounterToken replaceOneToken(CounterToken newToken) {
            assert (tokenList != null);
            CounterToken replaced = tokenList;
            tokenList = newToken;
            newToken.prev = null;
            newToken.next = replaced.next;

            if (newToken.next != null) {
                newToken.next.prev = newToken;
            }

            newToken.group = this;
            return replaced;
        }

        public void removeToken(CounterToken token) {


            if (token.prev != null) {
                token.prev.next = token.next;
            }

            if (token.next != null) {
                // head of list
                if (tokenList == token) {
                    assert (token.prev == null);
                    tokenList = token.next;
                    token.next.prev = null;
                } else {
                    token.next.prev = token.prev;
                }
            } else {
                if (tokenList == token) {
                    tokenList = null;
                }
            }

            token.next = null;
            token.prev = null;
            token.group = null;

            removeIfEmpty();
        }
    }

    private class CounterToken {
        private int item;
        private CounterGroup group;
        private CounterToken prev;
        private CounterToken next;

        private CounterToken(int item, CounterGroup group, CounterToken next) {
            this.item = item;
            this.group = group;
            this.next = next;
        }
    }

    @Override
    public double getCount(int item) {
        CounterToken token = digest.get(item);

        if (token == null) {
            return groupHead.differential;
        }

        CounterGroup group = token.group;

        double ret = 0L;
        while (group != null) {
            ret += group.differential;
            group = group.prev;
        }

        return ret;
    }

    private void incrementCounter(CounterToken token,
                                  Double by) {
        CounterGroup currentGroup = token.group;

        // we are at the end of the groups (highest count)
        if (currentGroup.next == null) {

            // we are the only matching token
            if (currentGroup.tokenList == token && token.next == null) {
                currentGroup.differential += by;
            }
            // insert a new token at the end
            else {
                currentGroup.removeToken(token);
                CounterGroup newGroup = new CounterGroup(by,
                                                         currentGroup,
                                                         null,
                                                         token);
                currentGroup.next = newGroup;

            }
        }
        // perfect match on next group
        else if (currentGroup.next.differential == by) {
            currentGroup.removeToken(token);
            currentGroup.next.addCounter(token);
        }
        // we are between current and current.next; insert a new token
        else if (currentGroup.next != null && currentGroup.next.differential > by) {
            currentGroup.next.differential -= by;

            // if this is the only token in the list, just increment
            if (currentGroup.tokenList == token && token.next == null) {
                currentGroup.differential += by;
            } else {
                currentGroup.removeToken(token);

                CounterGroup newGroup = new CounterGroup(by,
                                                         currentGroup,
                                                         currentGroup.next,
                                                         token);
                currentGroup.next.prev = newGroup;
                currentGroup.next = newGroup;
            }
        }
        // we fall after the next token in line
        else if (currentGroup.next != null && currentGroup.next.differential < by) {
            currentGroup.removeToken(token);

            insertNextMatchingGroup(currentGroup, token, by);
        }
    }

    private void insertNextMatchingGroup(CounterGroup currentGroup,
                                         CounterToken token,
                                         double remaining) {
        // find the group we want to insert ourselves after
        CounterGroup gteGroup = currentGroup.next;
        CounterGroup prevGroup = null;
        while (remaining > 0 && gteGroup != null) {
            remaining -= gteGroup.differential;
            prevGroup = gteGroup;
            gteGroup = gteGroup.next;
            assert (gteGroup == null || gteGroup.prev == prevGroup);
        }

        // we should be the new tail node
        if (remaining > 0) {
            CounterGroup newGroup = new CounterGroup(remaining,
                                                     prevGroup,
                                                     null,
                                                     token);
            if (prevGroup != null) {
                prevGroup.next = newGroup;
            }
        }
        // we matched exactly!
        else if (remaining == 0) {
            prevGroup.addCounter(token);
        }
        // insert ourselves before the last node we visited
        else {
            double delta = -remaining;
            // insert ourselves before...
            prevGroup.differential -= delta;
            CounterGroup newGroup = new CounterGroup(delta,
                                                     prevGroup.prev,
                                                     prevGroup,
                                                     token);

            if (prevGroup == groupHead) {
                groupHead = newGroup;
                prevGroup.prev = newGroup;
            } else {
                prevGroup.prev.next = newGroup;
                prevGroup.prev = newGroup;
            }
        }
    }

    @Override
    public void observe(Integer item, double count) {
        //sanityCheck();
        totalCount += count;

        CounterToken token = digest.get(item);
        if (token == null) {
            token = new CounterToken(item, null, null);
            digest.put(item, token);

            if (digest.size() <= maxSize) {
                // first group in the bunch
                if (groupHead == null) {
                    groupHead = new CounterGroup(count,
                                                 null,
                                                 null,
                                                 token);
                }
                // insert the token at the appropriate position
                else {
                    if (count == groupHead.differential) {
                        groupHead.addCounter(token);
                    }
                    // insert ourselves at the head of the list
                    else if (count < groupHead.differential) {
                        CounterGroup newGroup = new CounterGroup(count,
                                                                 null,
                                                                 groupHead,
                                                                 token);
                        groupHead.differential -= count;
                        groupHead.prev = newGroup;
                        groupHead = newGroup;
                    } else {
                        insertNextMatchingGroup(groupHead, token, count);
                    }
                }
            } else {
                CounterToken replaced = groupHead.replaceOneToken(token);
                incrementCounter(token, 1.0);
                digest.remove(replaced.item);
            }
        } else {
            incrementCounter(token, count);
        }
        //sanityCheck();
    }

    @Override
    public Map<Integer, Double> getCounts() {
        Map<Integer, Double> ret = new HashMap<>();

        CounterGroup curGroup = groupHead;
        Double currentCount = 0.;

        while (curGroup != null) {
            currentCount += curGroup.differential;
            CounterToken curToken = curGroup.tokenList;
            while (curToken != null) {
                ret.put(curToken.item, currentCount);
                curToken = curToken.next;
            }

            curGroup = curGroup.next;
        }

        return ret;
    }

    @Override
    public void multiplyAllCounts(Double by) {
        totalCount *= by;

        CounterGroup curGroup = groupHead;
        while (curGroup != null) {
            curGroup.differential *= by;
            curGroup = curGroup.next;
        }
    }

    public void debugPrint() {
        double currentCount = 0;
        CounterGroup curGroup = groupHead;
        log.debug("****");
        while (curGroup != null) {
            currentCount += curGroup.differential;
            log.debug("at group {}", curGroup);
            log.debug("differential is {}, so far is {}",
                      curGroup.differential,
                      currentCount);
            CounterToken token = curGroup.tokenList;
            while (token != null) {
                log.debug("\t counter: {}", token.item);
                token = token.next;
            }
            curGroup = curGroup.next;
            log.debug("");
        }
        log.debug("****");
    }

    public SpaceSavingList(int maxSize) {
        this.maxSize = maxSize;
    }
}
