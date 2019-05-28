package edu.berkeley.cs186.database.concurrency;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This class represents the full name of a resource. The name
 * of a resource is an ordered tuple of objects, and any
 * subsequence of the tuple starting with the first element
 * is the name of a resource higher up on the hierarchy.
 *
 * For example, a page may have the name ("database", "Students", 3L),
 * and its ancestors on the hierarchy would be ("database") [which
 * represents the entire database], and ("database", "Students") [which
 * represents the Students table, of which this is a page of].
 */
public class ResourceName {
    private List<Object> names;

    public ResourceName(Object name) {
        names = Arrays.asList(name);
    }
    public ResourceName(List<Object> names) {
        this.names = new ArrayList<>(names);
    }
    public ResourceName(List<Object> parents, Object name) {
        names = new ArrayList<>(parents);
        names.add(name);
    }
    public ResourceName(ResourceName parent, Object name) {
        names = new ArrayList<>(parent.names);
        names.add(name);
    }
    public ResourceName parent() {
        if (names.size() > 1) {
            return new ResourceName(names.subList(0, names.size() - 1));
        } else {
            return null;
        }
    }
    public boolean isChildOf(ResourceName other) {
        if (other.names.size() >= names.size()) {
            return false;
        }
        Iterator<Object> mine = names.iterator();
        Iterator<Object> others = other.names.iterator();
        while (others.hasNext()) {
            if (!mine.next().equals(others.next())) {
                return false;
            }
        }
        return true;
    }
    public List<Object> getNames() {
        return names;
    }
    @Override
    public boolean equals(Object other) {
        if (other instanceof ResourceName) {
            ResourceName n = (ResourceName) other;
            return n.names.equals(this.names);
        } else {
            return false;
        }
    }
    @Override
    public int hashCode() {
        return names.hashCode();
    }
    @Override
    public String toString() {
        String n = names.get(0).toString();
        for (int i = 1; i < names.size(); ++i) {
            n += "/" + names.get(i).toString();
        }
        return n;
    }
}

