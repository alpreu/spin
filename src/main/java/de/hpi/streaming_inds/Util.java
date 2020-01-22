package de.hpi.streaming_inds;

import akka.actor.ActorPath;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class Util {

    public static int getIdFromPath(ActorPath path) {
        String name = path.name();
        int lastUnderscoreIndex = name.lastIndexOf('_');
        if (lastUnderscoreIndex == -1) System.out.println("ActorPath did not contain underscore");
        return Integer.valueOf(name.substring(lastUnderscoreIndex + 1));
    }

    public static List<List<Integer>> calculateColumnDistribution(int numberOfColumns, int numberOfNodes) {
        List<Integer> colIds = IntStream.range(0, numberOfColumns).boxed().collect(Collectors.toList());
        return chopIntoParts(colIds, numberOfNodes);
    }

    public static <T>List<List<T>> chopIntoParts(final List<T> ls, final int iParts) {
        final List<List<T>> lsParts = new ArrayList<List<T>>();
        final int iChunkSize = ls.size() / iParts;
        int iLeftOver = ls.size() % iParts;
        int iTake = iChunkSize;

        for(int i = 0, iT = ls.size(); i < iT; i += iTake) {
            if(iLeftOver > 0) {
                iLeftOver--;
                iTake = iChunkSize + 1;
            } else {
                iTake = iChunkSize;
            }
            lsParts.add(new ArrayList<T>(ls.subList(i, Math.min(iT, i + iTake))));
        }
        return lsParts;
    }

    public static long getPercentile(List<Long> sortedList, double percentile) {
        int idx = (int) Math.ceil((percentile / (double) 100) * (double) sortedList.size());
        return sortedList.get(idx-1);
    }



}
