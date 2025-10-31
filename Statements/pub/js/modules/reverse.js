/**
 * Reverse module
 */

import { $, show, hide } from '../utils.js';
import { REVERSE_LENDERS, MODES } from '../config.js';
import { renderLenders } from '../components/lender-tiles.js';
import { makeStaticLenders } from '../utils.js';
import { setComposerData } from '../components/composer.js';

export function activateReverse() {
  $("resultsTitle").textContent = "Reverse Lenders";
  $("stmtTitle").textContent = "Upload Bank Statements";
  $("stmtLabel").textContent = "Bank Statements (PDFs)";
  $("stmtHint").textContent = "Upload recent bank statements (no analysis needed).";
  $("btnStmtsMatch").style.display = "none";
  $("posBadge").style.display = "none";

  hide($("cardApp"));
  show($("cardStatements"));
  hide($("stmtAnalysis"));

  renderLenders(makeStaticLenders(REVERSE_LENDERS));
  show($("results"));
  show($("composer"));

  setComposerData(MODES.REV, null, null, null);
}
