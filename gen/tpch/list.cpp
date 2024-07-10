#include "gen/tpch/list.h"

namespace gen {

namespace tpch {

std::shared_ptr<const gen::List> GetTypesList() {
  static std::shared_ptr<const gen::List> result = [] {
    std::vector<std::string> syllable_1 = {"STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"};
    std::vector<std::string> syllable_2 = {"ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"};
    std::vector<std::string> syllable_3 = {"TIN", "NICKEL", "BRASS", "STEEL", "COPPER"};

    std::vector<gen::List::StringWithWeight> result;
    result.reserve(syllable_1.size() * syllable_2.size() * syllable_3.size());
    for (const auto& s1 : syllable_1) {
      for (const auto& s2 : syllable_2) {
        for (const auto& s3 : syllable_3) {
          result.emplace_back(gen::List::StringWithWeight{.string = s1 + " " + s2 + " " + s3, .weight = 1});
        }
      }
    }

    return std::make_shared<gen::List>("types", std::move(result));
  }();

  return result;
}

std::shared_ptr<const gen::List> GetContainersList() {
  static std::shared_ptr<const gen::List> result = [] {
    std::vector<std::string> syllable_1 = {"SM", "LG", "MED", "JUMBO", "WRAP"};
    std::vector<std::string> syllable_2 = {"CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"};

    std::vector<gen::List::StringWithWeight> result;
    result.reserve(syllable_1.size() * syllable_2.size());
    for (const auto& s1 : syllable_1) {
      for (const auto& s2 : syllable_2) {
        result.emplace_back(gen::List::StringWithWeight{.string = s1 + " " + s2, .weight = 1});
      }
    }

    return std::make_shared<gen::List>("types", std::move(result));
  }();

  return result;
}

std::shared_ptr<const gen::List> GetSegmentsList() {
  static std::shared_ptr<const gen::List> result = std::make_shared<gen::List>(
      "segments", std::vector<std::string>{"AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"});
  return result;
}

std::shared_ptr<const gen::List> GetPrioritiesList() {
  static std::shared_ptr<const gen::List> result = std::make_shared<gen::List>(
      "segments", std::vector<std::string>{"1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"});
  return result;
}

std::shared_ptr<const gen::List> GetInstructionsList() {
  static std::shared_ptr<const gen::List> result = std::make_shared<gen::List>(
      "instructions", std::vector<std::string>{"DELIVER IN PERSON", "COLLECT COD", "TAKE BACK RETURN", "NONE"});
  return result;
}

std::shared_ptr<const gen::List> GetModesList() {
  static std::shared_ptr<const gen::List> result = std::make_shared<gen::List>(
      "modes", std::vector<std::string>{"REG AIR", "AIR", "RAIL", "TRUCK", "MAIL", "FOB", "SHIP"});
  return result;
}

namespace text {

std::shared_ptr<const gen::List> GetNounsList() {
  static std::shared_ptr<const gen::List> list = std::make_shared<gen::List>(
      "nouns",
      std::vector<gen::List::StringWithWeight>{
          {"packages", 40},   {"requests", 40},    {"accounts", 40},    {"deposits", 40},     {"foxes", 20},
          {"ideas", 20},      {"theodolites", 20}, {"pinto beans", 20}, {"instructions", 20}, {"dependencies", 20},
          {"excuses", 10},    {"platelets", 10},   {"asymptotes", 10},  {"courts", 5},        {"dolphins", 5},
          {"multipliers", 1}, {"sauternes", 1},    {"warthogs", 1},     {"frets", 1},         {"dinos", 1},
          {"attainments", 1}, {"somas", 1},        {"Tiresias", 1},     {"patterns", 1},      {"forges", 1},
          {"braids", 1},      {"frays", 1},        {"warhorses", 1},    {"dugouts", 1},       {"notornis", 1},
          {"epitaphs", 1},    {"pearls", 1},       {"tithes", 1},       {"waters", 1},        {"orbits", 1},
          {"gifts", 1},       {"sheaves", 1},      {"depths", 1},       {"sentiments", 1},    {"decoys", 1},
          {"realms", 1},      {"pains", 1},        {"grouches", 1},     {"escapades", 1},     {"hockey players", 1},
      });

  return list;
}

std::shared_ptr<const gen::List> GetVerbsList() {
  static std::shared_ptr<const gen::List> list = std::make_shared<gen::List>(
      "verbs", std::vector<gen::List::StringWithWeight>{
                   {"sleep", 20},  {"wake", 20},  {"are", 20},    {"cajole", 20}, {"haggle", 20},   {"nag", 10},
                   {"use", 10},    {"boost", 10}, {"affix", 5},   {"detect", 5},  {"integrate", 1}, {"maintain", 1},
                   {"nod", 1},     {"was", 1},    {"lose", 1},    {"sublate", 1}, {"solve", 1},     {"thrash", 1},
                   {"promise", 1}, {"engage", 1}, {"hinder", 1},  {"print", 1},   {"x-ray", 1},     {"breach", 1},
                   {"eat", 1},     {"grow", 1},   {"impress", 1}, {"mold", 1},    {"poach", 1},     {"serve", 1},
                   {"run", 1},     {"dazzle", 1}, {"snooze", 1},  {"doze", 1},    {"unwind", 1},    {"kindle", 1},
                   {"play", 1},    {"hang", 1},   {"believe", 1}, {"doubt", 1},
               });

  return list;
}

std::shared_ptr<const gen::List> GetAdjectivesList() {
  // like tpch-kit implementation (it does not match list adjectives in 4.2.2.13)
  static std::shared_ptr<const gen::List> list = std::make_shared<gen::List>(
      "adjectives",
      std::vector<gen::List::StringWithWeight>{
          {"special", 20}, {"pending", 20},  {"unusual", 20}, {"express", 20}, {"furious", 1}, {"sly", 1},
          {"careful", 1},  {"blithe", 1},    {"quick", 1},    {"fluffy", 1},   {"slow", 1},    {"quiet", 1},
          {"ruthless", 1}, {"thin", 1},      {"close", 1},    {"dogged", 1},   {"daring", 1},  {"brave", 1},
          {"stealthy", 1}, {"permanent", 1}, {"enticing", 1}, {"idle", 1},     {"busy", 1},    {"regular", 50},
          {"final", 40},   {"ironic", 40},   {"even", 30},    {"bold", 20},    {"silent", 10},
      });

  return list;
}

std::shared_ptr<const gen::List> GetAdverbsList() {
  static std::shared_ptr<const gen::List> list = std::make_shared<gen::List>(
      "adverbs", std::vector<gen::List::StringWithWeight>{
                     {"sometimes", 1},  {"always", 1},     {"never", 1},      {"furiously", 50},  {"slyly", 50},
                     {"carefully", 50}, {"blithely", 40},  {"quickly", 30},   {"fluffily", 20},   {"slowly", 1},
                     {"quietly", 1},    {"ruthlessly", 1}, {"thinly", 1},     {"closely", 1},     {"doggedly", 1},
                     {"daringly", 1},   {"bravely", 1},    {"stealthily", 1}, {"permanently", 1}, {"enticingly", 1},
                     {"idly", 1},       {"busily", 1},     {"regularly", 1},  {"finally", 1},     {"ironically", 1},
                     {"evenly", 1},     {"boldly", 1},     {"silently", 1}});

  return list;
}

std::shared_ptr<const gen::List> GetPrepositionsList() {
  static std::shared_ptr<const gen::List> list =
      std::make_shared<gen::List>("prepositions", std::vector<gen::List::StringWithWeight>{
                                                      {"about", 50},
                                                      {"above", 50},
                                                      {"according to", 50},
                                                      {"across", 50},
                                                      {"after", 50},
                                                      {"against", 40},
                                                      {"along", 40},
                                                      {"alongside of", 30},
                                                      {"among", 30},
                                                      {"around", 20},
                                                      {"at", 10},
                                                      {"atop", 1},
                                                      {"before", 1},
                                                      {"behind", 1},
                                                      {"beneath", 1},
                                                      {"beside", 1},
                                                      {"besides", 1},
                                                      {"between", 1},
                                                      {"beyond", 1},
                                                      {"by", 1},
                                                      {"despite", 1},
                                                      {"during", 1},
                                                      {"except", 1},
                                                      {"for", 1},
                                                      {"from", 1},
                                                      {"in place of", 1},
                                                      {"inside", 1},
                                                      {"instead of", 1},
                                                      {"into", 1},
                                                      {"near", 1},
                                                      {"of", 1},
                                                      {"on", 1},
                                                      {"outside", 1},
                                                      {"over", 1},
                                                      {"past", 1},
                                                      {"since", 1},
                                                      {"through", 1},
                                                      {"throughout", 1},
                                                      {"to", 1},
                                                      {"toward", 1},
                                                      {"under", 1},
                                                      {"until", 1},
                                                      {"up", 1},
                                                      {"upon", 1},
                                                      {"whithout", 1},
                                                      {"with", 1},
                                                      {"within", 1},
                                                  });

  return list;
}

std::shared_ptr<const gen::List> GetAuxiliariesList() {
  static std::shared_ptr<const gen::List> list =
      std::make_shared<gen::List>("auxillaries", std::vector<gen::List::StringWithWeight>{
                                                     {"do", 1},
                                                     {"may", 1},
                                                     {"might", 1},
                                                     {"shall", 1},
                                                     {"will", 1},
                                                     {"would", 1},
                                                     {"can", 1},
                                                     {"could", 1},
                                                     {"should", 1},
                                                     {"ought to", 1},
                                                     {"must", 1},
                                                     {"will have to", 1},
                                                     {"shall have to", 1},
                                                     {"could have to", 1},
                                                     {"should have to", 1},
                                                     {"must have to", 1},
                                                     {"need to", 1},
                                                     {"try to", 1},
                                                 });

  return list;
}

std::shared_ptr<const gen::List> GetTerminatorsList() {
  static std::shared_ptr<const gen::List> list =
      std::make_shared<gen::List>("auxillaries", std::vector<gen::List::StringWithWeight>{
                                                     {".", 50},
                                                     {";", 1},
                                                     {":", 1},
                                                     {"?", 1},
                                                     {"!", 1},
                                                     {"--", 1},
                                                 });

  return list;
}

std::shared_ptr<const gen::List> GetSentenceGrammarList() {
  static std::shared_ptr<const gen::List> list = std::make_shared<gen::List>(
      "sentence_grammar", std::vector<gen::List::StringWithWeight>{
                              {"N V T", 3}, {"N V P T", 3}, {"N V N T", 3}, {"N P V N T", 1}, {"N P V P T", 1}});

  return list;
}

std::shared_ptr<const gen::List> GetNounPhraseGrammarList() {
  static std::shared_ptr<const gen::List> list = std::make_shared<gen::List>(
      "noun_phrase_grammar",
      std::vector<gen::List::StringWithWeight>{{"N", 10}, {"J N", 20}, {"J, J N", 10}, {"D J N", 50}});

  return list;
}

std::shared_ptr<const gen::List> GetVerbPhraseGrammarList() {
  static std::shared_ptr<const gen::List> list = std::make_shared<gen::List>(
      "verb_phrase_grammar",
      std::vector<gen::List::StringWithWeight>{{"V", 30}, {"X V", 1}, {"V D", 40}, {"X V D", 1}});

  return list;
}

}  // namespace text

namespace part {

std::shared_ptr<const gen::List> GetPNameList() {
  static std::shared_ptr<const gen::List> list = std::make_shared<gen::List>(
      "p_name_list",
      std::vector<std::string>{
          "almond",    "antique",    "aquamarine", "azure",     "beige",     "bisque",     "black",     "blanched",
          "blue",      "blush",      "brown",      "burlywood", "burnished", "chartreuse", "chiffon",   "chocolate",
          "coral",     "cornflower", "cornsilk",   "cream",     "cyan",      "dark",       "deep",      "dim",
          "dodger",    "drab",       "firebrick",  "floral",    "forest",    "frosted",    "gainsboro", "ghost",
          "goldenrod", "green",      "grey",       "honeydew",  "hot",       "indian",     "ivory",     "khaki",
          "lace",      "lavender",   "lawn",       "lemon",     "light",     "lime",       "linen",     "magenta",
          "maroon",    "medium",     "metallic",   "midnight",  "mint",      "misty",      "moccasin",  "navajo",
          "navy",      "olive",      "orange",     "orchid",    "pale",      "papaya",     "peach",     "peru",
          "pink",      "plum",       "powder",     "puff",      "purple",    "red",        "rose",      "rosy",
          "royal",     "saddle",     "salmon",     "sandy",     "seashell",  "sienna",     "sky",       "slate",
          "smoke",     "snow",       "spring",     "steel",     "tan",       "thistle",    "tomato",    "turquoise",
          "violet",    "wheat",      "white",      "yellow"});

  return list;
}

}  // namespace part

}  // namespace tpch

}  // namespace gen
