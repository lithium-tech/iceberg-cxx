#include "gen/tpch/text.h"

#include "arrow/type_fwd.h"
#include "gen/src/generators.h"
#include "gen/tpch/list.h"

namespace gen::tpch::text {

std::string GenerateVerbPhrase(gen::StringFromListGenerator& verb_phrase_grammar_gen,
                               gen::StringFromListGenerator& verb_gen, gen::StringFromListGenerator& auxiliary_gen,
                               gen::StringFromListGenerator& adverb_gen) {
  std::string result;
  std::string verb_phrase_grammar = verb_phrase_grammar_gen.GenerateValue();
  for (const char verb_phrase_grammar_char : verb_phrase_grammar) {
    switch (verb_phrase_grammar_char) {
      case ' ':
        result += " ";
        break;
      case 'V':
        result += verb_gen.GenerateValue();
        break;
      case 'X':
        result += auxiliary_gen.GenerateValue();
        break;
      case 'D':
        result += adverb_gen.GenerateValue();
        break;
      default:
        throw std::runtime_error(std::string("Internal error: ") + __FILE__ + ":" + std::to_string(__LINE__));
    }
  }

  return result;
}

std::string GenerateNounPhrase(gen::StringFromListGenerator& noun_phrase_grammar_gen,
                               gen::StringFromListGenerator& noun_gen, gen::StringFromListGenerator& adjective_gen,
                               gen::StringFromListGenerator& adverb_gen) {
  std::string result;
  std::string noun_phrase_grammar = noun_phrase_grammar_gen.GenerateValue();
  for (const char noun_phrase_grammar_char : noun_phrase_grammar) {
    switch (noun_phrase_grammar_char) {
      case ' ':
        result += " ";
        break;
      case 'N':
        result += noun_gen.GenerateValue();
        break;
      case 'J':
        result += adjective_gen.GenerateValue();
        break;
      case 'D':
        result += adverb_gen.GenerateValue();
        break;
      case ',':
        result += ",";
        break;
      default:
        throw std::runtime_error(std::string("Internal error: ") + __FILE__ + ":" + std::to_string(__LINE__));
    }
  }

  return result;
}

Text GenerateText(gen::RandomDevice& random_device) {
  auto nouns_list = GetNounsList();
  auto verbs_list = GetVerbsList();
  auto adjectives_list = GetAdjectivesList();
  auto adverbs_list = GetAdverbsList();
  auto prepositions_list = GetPrepositionsList();
  auto auxiliaries_list = GetAuxiliariesList();
  auto terminators_list = GetTerminatorsList();
  auto sentence_grammar_list = GetSentenceGrammarList();
  auto verb_phrase_grammar_list = GetVerbPhraseGrammarList();
  auto noun_phrase_grammar_list = GetNounPhraseGrammarList();

  gen::StringFromListGenerator sentence_grammar_gen(*sentence_grammar_list, random_device);
  gen::StringFromListGenerator noun_phrase_grammar_gen(*noun_phrase_grammar_list, random_device);
  gen::StringFromListGenerator verb_phrase_grammar_gen(*verb_phrase_grammar_list, random_device);
  gen::StringFromListGenerator terminator_gen(*terminators_list, random_device);
  gen::StringFromListGenerator noun_gen(*nouns_list, random_device);
  gen::StringFromListGenerator adjective_gen(*adjectives_list, random_device);
  gen::StringFromListGenerator adverb_gen(*adverbs_list, random_device);
  gen::StringFromListGenerator verb_gen(*verbs_list, random_device);
  gen::StringFromListGenerator auxiliary_gen(*auxiliaries_list, random_device);
  gen::StringFromListGenerator preposition_gen(*prepositions_list, random_device);

  constexpr int64_t kTextMinimumLength = 300 * 1024 * 1024;
  std::string text;
  text.reserve(kTextMinimumLength);

  while (text.size() < kTextMinimumLength) {
    std::string grammar = sentence_grammar_gen.GenerateValue();
    std::string result;
    for (const char grammar_char : grammar) {
      switch (grammar_char) {
        case ' ':
          result += " ";
          break;
        case 'V':
          result += GenerateVerbPhrase(verb_phrase_grammar_gen, verb_gen, auxiliary_gen, adverb_gen);
          break;
        case 'P': {
          result += preposition_gen.GenerateValue();
          result += " the ";
          [[fallthrough]];
        }
        case 'N':
          result += GenerateNounPhrase(noun_phrase_grammar_gen, noun_gen, adjective_gen, adverb_gen);
          break;
        case 'T':
          result.pop_back();
          result += terminator_gen.GenerateValue();
          break;
        default:
          throw std::runtime_error(std::string("Internal error: ") + __FILE__ + ":" + std::to_string(__LINE__));
      }
    }

    text += std::move(result);
    text += " ";
  }

  text.pop_back();

  return text;
}

}  // namespace gen::tpch::text
