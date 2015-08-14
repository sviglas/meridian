package net.sviglas.meridian.task;

import java.util.Iterator;

class LNode<K> {
    public K value;
    public LNode<K> next;

    public LNode(K k) { value = k; next = null; }
}

public class LList<K> {
    private LNode<K> head;
    private LNode<K> tail;
    private int size;

    public LList() {
        head = null;
        tail = null;
        size = 0;
    }

    public LList<K> append(K k) {
        LNode<K> node = new LNode<>(k);
        if (head != null) {
            tail.next = node;
            tail = node;
        }
        else {
            head = node;
            tail = node;
        }
        size++;
        return this;
    }

    public LList<K> append(LList<K> lst) {
        if (head != null) {
            tail.next = lst.head;
            tail = lst.tail;
        }
        else {
            head = lst.head;
            tail = lst.tail;
        }
        size += lst.size();
        return this;
    }

    public int size() {
        return size;
    }

    public Iterator<K> iterator() {
        return new LIterator();
    }

    public String toString() {
        if (size() == 0) return "";
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (Iterator<K> it = iterator(); it.hasNext();)
            sb.append(it.next()).append(",");
        sb.setLength(sb.length()-1);
        sb.append("]");
        return sb.toString();
    }

    class LIterator implements Iterator<K> {
        private LNode<K> node;
        public LIterator() { node = head; }
        public boolean hasNext() { return node != null; }
        public K next() { K k = node.value; node = node.next; return k; }
        public void remove() {}
    }

    public static void main(String [] args) {
        LList<Integer> one = new LList<>();
        int i;
        for (i = 0; i < 10; i++) one.append(i);
        LList<Integer> two = new LList<>();
        for (; i < 30; i++) two.append(i);
        LList<Integer> three = new LList<>();
        for (; i < 60; i++) three.append(i);
                
        one.append(two);
        one.append(three);
        
        Iterator<Integer> iterator = one.iterator();
        while (iterator.hasNext()) System.out.println(iterator.next());

        System.out.println(one);
        System.out.println(one.size());
    }
}
