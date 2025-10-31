/**
 * CCS (Credit Card Split) module
 */

import { $, show, hide } from '../utils.js';
import { CCS_LENDERS, MODES } from '../config.js';
import { renderLenders } from '../components/lender-tiles.js';
import { makeStaticLenders } from '../utils.js';
import { setComposerData } from '../components/composer.js';

export function activateCCS() {
  $("resultsTitle").textContent = "Credit Card Split Lenders";
  $("stmtTitle").textContent = "Upload Credit Card Statements";
  $("stmtLabel").textContent = "Credit Card Processor Statements (PDFs)";
  $("stmtHint").textContent = "Upload recent processor statements (no analysis needed).";
  $("btnStmtsMatch").style.display = "none";
  $("posBadge").style.display = "none";

  hide($("cardApp"));
  show($("cardStatements"));
  hide($("stmtAnalysis"));

  renderLenders(makeStaticLenders(CCS_LENDERS));
  show($("results"));
  show($("composer"));

  setComposerData(MODES.CCS, null, null, null);
}
