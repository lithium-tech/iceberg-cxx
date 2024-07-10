#pragma once

#include <memory>

#include "gen/src/list.h"

namespace gen {

namespace tpch {

std::shared_ptr<const gen::List> GetTypesList();
std::shared_ptr<const gen::List> GetContainersList();
std::shared_ptr<const gen::List> GetSegmentsList();
std::shared_ptr<const gen::List> GetPrioritiesList();
std::shared_ptr<const gen::List> GetInstructionsList();
std::shared_ptr<const gen::List> GetModesList();

namespace text {

std::shared_ptr<const gen::List> GetNounsList();
std::shared_ptr<const gen::List> GetVerbsList();
std::shared_ptr<const gen::List> GetAdjectivesList();
std::shared_ptr<const gen::List> GetAdverbsList();
std::shared_ptr<const gen::List> GetPrepositionsList();
std::shared_ptr<const gen::List> GetAuxiliariesList();
std::shared_ptr<const gen::List> GetTerminatorsList();
std::shared_ptr<const gen::List> GetSentenceGrammarList();
std::shared_ptr<const gen::List> GetNounPhraseGrammarList();
std::shared_ptr<const gen::List> GetVerbPhraseGrammarList();

}  // namespace text

namespace part {

std::shared_ptr<const gen::List> GetPNameList();

}

}  // namespace tpch

}  // namespace gen
