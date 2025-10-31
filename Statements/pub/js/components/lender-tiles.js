/**
 * Lender tiles component
 */

import { $} from '../utils.js';
import { AUTO_SELECT } from '../config.js';

export const selectedLenders = new Set();

export function clearResults() {
  $("lendersGrid").innerHTML = "";
  $("selectedCount").textContent = "0 selected";
}

export function renderLenders(list) {
  const grid = $("lendersGrid");
  grid.innerHTML = "";
  selectedLenders.clear();
  $("selectedCount").textContent = "0 selected";

  if (!Array.isArray(list) || list.length === 0) {
    const note = document.createElement("div");
    note.className = "note-err";
    note.textContent = "No lenders matched.";
    grid.appendChild(note);
    return;
  }

  let autoSelected = 0;

  list.forEach(item => {
    const name = item.business_name || "(Unnamed)";
    let scoreNum = Number(item.score);
    if (!isFinite(scoreNum)) scoreNum = 0;
    const scorePct = Math.round(scoreNum * 100);
    const reason = item.reason || "";

    const tile = document.createElement("div");
    tile.className = "tile";
    tile.title = reason;
    tile.innerHTML = `<div class="name">${name}</div><div class="meta">${scorePct}%</div>`;

    const eligible = scoreNum > AUTO_SELECT.minScore;
    if (AUTO_SELECT.enabled && eligible && autoSelected < AUTO_SELECT.maxSelection) {
      tile.classList.add("selected");
      selectedLenders.add(name);
      autoSelected += 1;
    }

    tile.addEventListener("click", () => {
      if (tile.classList.contains("selected")) {
        tile.classList.remove("selected");
        selectedLenders.delete(name);
      } else {
        tile.classList.add("selected");
        selectedLenders.add(name);
      }
      $("selectedCount").textContent = `${selectedLenders.size} selected`;
    });

    grid.appendChild(tile);
  });

  $("selectedCount").textContent = `${selectedLenders.size} selected`;
}
