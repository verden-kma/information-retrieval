package ukma.ir.data_stuctores;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class RandomizedQueue<Item> implements Iterable<Item> {

    private static final double SHRINK_COEFF = .25;
    private static final double RESIZE_COEFF = .5;

    private Item[] content = (Item[]) new Object[10];
    private int tail;

    public RandomizedQueue() {
    }

    public boolean isEmpty() {
        return tail == 0;
    }

    public int size() {
        return tail;
    }

    /**
     * add the item
     *
     * @param item
     */
    public void enqueue(Item item) {
        if (item == null) throw new IllegalArgumentException();
        if (content.length == tail) resize((int) (content.length * (1.0 + RESIZE_COEFF)));
        content[tail++] = item;
    }

    /**
     * @return and remove a random item
     */
    public Item dequeue() {
        if (isEmpty()) throw new NoSuchElementException();
        int index = (int) (Math.random() * tail);
        Item item = content[index];
        content[index] = content[--tail];
        if (tail <= content.length * SHRINK_COEFF) resize((int) (content.length * (1.0 - RESIZE_COEFF)));
        return item;
    }

    private void resize(int newLength) {
        if (newLength <= 10) return;
        Item[] newContent = (Item[]) new Object[newLength];
        System.arraycopy(content, 0, newContent, 0, tail);
        content = newContent;
    }

    /**
     * @return a random item (but do not remove it)
     */
    public Item sample() {
        if (isEmpty()) throw new NoSuchElementException();
        return content[(int) (Math.random() * tail)];
    }

    /**
     * @return an independent iterator over items in random order
     */
    public Iterator<Item> iterator() {
        return new Iterator<Item>() {
            //contains indices of not shown elements
            int[] provided = new int[tail];
            int numProvided;
            Item[] localContent = (Item[]) new Object[content.length];
            {
                System.arraycopy(content, 0, localContent, 0, content.length);
            }

            @Override
            public boolean hasNext() {
                return numProvided < provided.length;
            }

            @Override
            public Item next() {
                if (!hasNext()) throw new NoSuchElementException();
                int index = (int) (Math.random() * (provided.length - numProvided));
                Item item = localContent[index];
                numProvided++;
                localContent[index] = localContent[provided.length - numProvided];
                return item;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
