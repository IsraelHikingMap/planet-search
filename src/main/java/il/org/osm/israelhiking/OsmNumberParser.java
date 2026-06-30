package il.org.osm.israelhiking;

import java.util.Locale;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class OsmNumberParser {

  private static final Pattern FIRST_NUMBER = Pattern.compile("-?\\d[\\d ,']*(?:\\.\\d+)?");
  private static final Pattern POPULATION_DIGITS = Pattern.compile("-?\\d[\\d ,.']*");
  private static final double FEET_TO_METERS = 0.3048;

  private OsmNumberParser() {}

  static OptionalInt parsePopulation(String raw) {
    if (raw == null) {
      return OptionalInt.empty();
    }
    Matcher matcher = POPULATION_DIGITS.matcher(raw);
    if (!matcher.find()) {
      return OptionalInt.empty();
    }
    try {
      long value = Long.parseLong(matcher.group().replaceAll("[ ,.']", ""));
      if (value <= 0) {
        return OptionalInt.empty();
      }
      return OptionalInt.of((int) Math.min(value, Integer.MAX_VALUE));
    } catch (NumberFormatException e) {
      return OptionalInt.empty();
    }
  }

  static OptionalDouble parseElevation(String raw) {
    double n = firstNumber(raw);
    if (Double.isNaN(n)) {
      return OptionalDouble.empty();
    }
    return OptionalDouble.of(isFeet(raw) ? n * FEET_TO_METERS : n);
  }

  private static boolean isFeet(String raw) {
    String t = raw.toLowerCase(Locale.ROOT).trim();
    return t.endsWith("ft") || t.endsWith("feet") || t.endsWith("foot") || t.endsWith("'");
  }

  private static double firstNumber(String raw) {
    if (raw == null) {
      return Double.NaN;
    }
    Matcher matcher = FIRST_NUMBER.matcher(raw);
    if (!matcher.find()) {
      return Double.NaN;
    }
    try {
      return Double.parseDouble(matcher.group().replaceAll("[ ,']", ""));
    } catch (NumberFormatException e) {
      return Double.NaN;
    }
  }
}
