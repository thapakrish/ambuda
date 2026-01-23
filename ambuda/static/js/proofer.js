/* global Alpine, $, OpenSeadragon, Sanscript, IMAGE_URL, OCR_BOUNDING_BOXES */
/* Transcription and proofreading interface. */

import { $ } from './core.ts';
import ProofingEditor, { XMLView } from './prosemirror-editor.ts';
import { INLINE_MARKS } from './marks-config.ts';
import routes from './routes.js';

const CONFIG_KEY = 'proofing-editor';

const ImageLayout = {
  Left: 'image-left',
  Right: 'image-right',
  Top: 'image-top',
  Bottom: 'image-bottom',
};
const ALL_LAYOUTS = [ImageLayout.Left, ImageLayout.Right, ImageLayout.Top, ImageLayout.Bottom];

const ImageClasses = {
  Left: 'flex flex-row-reverse h-[90vh]',
  Right: 'flex flex-row h-[90vh]',
  Top: 'flex flex-col-reverse h-[90vh]',
  Bottom: 'flex flex-col h-[90vh]',
};

const ViewType = {
  Visual: 'visual',
  XML: 'xml',
};

const ModalType = {
  CommandPalette: 'command-palette',
  History: 'history',
  Submit: 'submit',
  Normalize: 'normalize',
  Transliterate: 'transliterate',
  AutoStructure: 'auto-structure',
};

// Parse OCR bounding boxes from TSV
function parseBoundingBoxes(tsvData) {
  if (!tsvData) return [];

  const lines = tsvData.trim().split('\n');
  return lines.map(line => {
    const parts = line.split('\t');
    if (parts.length >= 5) {
      return {
        x1: parseInt(parts[0], 10),
        y1: parseInt(parts[1], 10),
        x2: parseInt(parts[2], 10),
        y2: parseInt(parts[3], 10),
        text: parts[4],
      };
    }
    return null;
  }).filter(box => box !== null);
}

// Calculate Levenshtein distance between two strings
function levenshteinDistance(str1, str2) {
  const len1 = str1.length;
  const len2 = str2.length;

  const dp = Array(len1 + 1).fill(null).map(() => Array(len2 + 1).fill(0));

  for (let i = 0; i <= len1; i++) {
    dp[i][0] = i;
  }
  for (let j = 0; j <= len2; j++) {
    dp[0][j] = j;
  }

  for (let i = 1; i <= len1; i++) {
    for (let j = 1; j <= len2; j++) {
      if (str1[i - 1] === str2[j - 1]) {
        dp[i][j] = dp[i - 1][j - 1];
      } else {
        dp[i][j] = Math.min(
          dp[i - 1][j] + 1,      // deletion
          dp[i][j - 1] + 1,      // insertion
          dp[i - 1][j - 1] + 1   // substitution
        );
      }
    }
  }

  return dp[len1][len2];
}

// Calculate similarity ratio between two strings (0 to 1, where 1 is identical)
function similarityRatio(str1, str2) {
  if (str1 === str2) return 1.0;
  if (!str1 || !str2) return 0.0;

  const distance = levenshteinDistance(str1, str2);
  const maxLen = Math.max(str1.length, str2.length);

  return 1 - (distance / maxLen);
}

// Group bounding boxes by line based on y-coordinate
function groupBoundingBoxesByLine(boxes) {
  if (!boxes || boxes.length === 0) return [];

  const Y_SENSITIVITY = 10;
  const sortedBoxes = [...boxes].sort((a, b) => {
    const yDiff = a.y1 - b.y1;
    if (Math.abs(yDiff) < Y_SENSITIVITY) {
       // If y-coordinates are very close, sort by x
      return a.x1 - b.x1;
    }
    return yDiff;
  });

  const lines = [];
  let currentLine = [sortedBoxes[0]];
  let currentLineY = sortedBoxes[0].y1;

  // Words on the same line should have similar y-coordinates
  for (let i = 1; i < sortedBoxes.length; i++) {
    const box = sortedBoxes[i];
    const yDiff = Math.abs(box.y1 - currentLineY);

    if (yDiff < Y_SENSITIVITY) {
      currentLine.push(box);
    } else {
      lines.push(currentLine);
      currentLine = [box];
      currentLineY = box.y1;
    }
  }

  if (currentLine.length > 0) {
    lines.push(currentLine);
  }

  return lines.map(lineBoxes => {
    const text = lineBoxes.map(box => box.text).join(' ');
    return {
      text: text,
      boxes: lineBoxes,
    };
  });
}

/* Initialize our image viewer. */
function initializeImageViewer(imageURL) {
  return OpenSeadragon({
    id: 'osd-image',
    tileSources: {
      type: 'image',
      url: imageURL,
      buildPyramid: false,
    },

    // Buttons
    showZoomControl: false,
    showHomeControl: false,
    showRotationControl: true,
    showFullPageControl: false,
    // Zoom buttons are defined in the `Editor` component below.
    rotateLeftButton: 'osd-rotate-left',
    rotateRightButton: 'osd-rotate-right',

    // Animations
    gestureSettingsMouse: {
      flickEnabled: true,
    },
    animationTime: 0.5,

    // The zoom multiplier to use when using the zoom in/out buttons.
    zoomPerClick: 1.1,
    // Max zoom level
    maxZoomPixelRatio: 2.5,
  });
}

export default () => ({
  // Settings
  textZoom: 1,
  imageZoom: null,
  layout: 'image-right',
  viewMode: ViewType.Visual,
  // [transliteration] the source script
  fromScript: 'hk',
  // [transliteration] the destination script
  toScript: 'devanagari',
  // If true, show advanced options (text, n, and merge_next)
  showAdvancedOptions: false,

  // Internal-only
  layoutClasses: ImageClasses.Right,
  isRunningOCR: false,
  isRunningLLMStructuring: false,
  isRunningStructuring: false,
  hasUnsavedChanges: false,
  xmlParseError: null,
  imageViewer: null,
  editor: null,
  // Modal state - only one modal open at a time
  activeModal: null,
  commandPaletteQuery: '',
  commandPaletteSelected: 0,
  historyLoading: false,
  historyRevisions: [],
  modalSummary: '',
  modalStatus: '',
  originalContent: '',
  changesPreview: '',
  // Normalize modal options
  normalizeReplaceColonVisarga: true,
  normalizeReplaceSAvagraha: true,
  normalizeReplaceDoublePipe: true,
  // Auto-structure modal options
  autoStructureStageDirections: true,
  autoStructureSpeakers: true,
  autoStructureChaya: true,
  // OCR bounding box highlighting
  boundingBoxes: [],
  boundingBoxLines: [],
  currentOverlay: null,

  init() {
    this.loadSettings();
    this.layoutClasses = this.getLayoutClasses();

    // OCR bounding boxes (rendered on OSD image viewer)
    this.boundingBoxes = parseBoundingBoxes(OCR_BOUNDING_BOXES);
    this.boundingBoxLines = groupBoundingBoxesByLine(this.boundingBoxes);

    // Initialize editor (either ProofingEditor or XMLView based on viewMode)
    const editorElement = $('#prosemirror-editor');
    const initialContent = $('#content').value || '';
    this.originalContent = initialContent;

    // NOTE: always use Alpine.raw() to access the editor because Alpine reactivity/proxies breaks
    // the underlying data model and causes bizarre errors, e.g.:
    //
    // https://discuss.prosemirror.net/t/getting-rangeerror-applying-a-mismatched-transaction-even-with-trivial-code/4948/3
    if (this.viewMode === ViewType.XML) {
      this.editor = new XMLView(editorElement, initialContent, () => {
        this.hasUnsavedChanges = true;
        $('#content').value = Alpine.raw(this.editor).getText();
      });
    } else {
      this.editor = new ProofingEditor(editorElement, initialContent, () => {
        this.hasUnsavedChanges = true;
        $('#content').value = Alpine.raw(this.editor).getText();
      }, this.showAdvancedOptions, this.textZoom, (context) => {
        this.onActiveWordChange(context);
      });
    }

    // Set `imageZoom` only after the viewer is fully initialized.
    this.imageViewer = initializeImageViewer(IMAGE_URL);
    this.imageViewer.addHandler('open', () => {
      this.imageZoom = this.imageZoom || this.imageViewer.viewport.getHomeZoom();
      this.imageViewer.viewport.zoomTo(this.imageZoom);
    });

    // Use `.bind(this)` so that `this` in the function refers to this app and
    // not `window`.
    window.onbeforeunload = this.onBeforeUnload.bind(this);
  },

  getCommands() {
    const markCommands = INLINE_MARKS.map(mark => ({
      label: `Edit > ${mark.label}`,
      action: () => this.toggleMark(mark.name)
    }));

    return [
      { label: 'Edit > Undo', action: () => this.undo() },
      { label: 'Edit > Redo', action: () => this.redo() },
      { label: 'Edit > Insert block', action: () => this.insertBlock() },
      { label: 'Edit > Delete active block', action: () => this.deleteBlock() },
      { label: 'Edit > Move block up', action: () => this.moveBlockUp() },
      { label: 'Edit > Move block down', action: () => this.moveBlockDown() },
      ...markCommands,
      { label: 'View > Show image on left', action: () => this.displayImageOnLeft() },
      { label: 'View > Show image on right', action: () => this.displayImageOnRight() },
      { label: 'View > Show image on top', action: () => this.displayImageOnTop() },
      { label: 'View > Show image on bottom', action: () => this.displayImageOnBottom() },
      { label: 'Tools > Normalize', action: () => this.openNormalizeModal() },
      { label: 'Tools > Transliterate', action: () => this.openTransliterateModal() },
      { label: 'Tools > Auto-structure', action: () => this.openAutoStructureModal() },
    ];
  },

  getFilteredCommands() {
    const query = this.commandPaletteQuery.toLowerCase();
    if (!query) return this.getCommands();
    return this.getCommands().filter(cmd =>
      cmd.label.toLowerCase().includes(query)
    );
  },

  openCommandPalette() {
    this.activeModal = ModalType.CommandPalette;
    this.commandPaletteQuery = '';
    this.commandPaletteSelected = 0;

    this.$nextTick(() => {
      // requestAnimationFrame ensures the browser has painted the modal
      requestAnimationFrame(() => {
        const input = document.querySelector('#command-palette-input');
        if (input) {
          input.focus();
        }
      });
    });
  },

  closeModal() {
    this.activeModal = null;
  },

  handleCommandPaletteKeydown(e) {
    const filtered = this.getFilteredCommands();
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      this.commandPaletteSelected = Math.min(this.commandPaletteSelected + 1, filtered.length - 1);
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      this.commandPaletteSelected = Math.max(this.commandPaletteSelected - 1, 0);
    } else if (e.key === 'Enter') {
      e.preventDefault();
      this.executeSelectedCommand();
    } else if (e.key === 'Escape') {
      e.preventDefault();
      this.closeModal();
    }
  },

  executeCommand(index) {
    const filtered = this.getFilteredCommands();
    if (filtered[index]) {
      filtered[index].action();
      if (this.activeModal === ModalType.CommandPalette) {
        // without this guard, alpine closes the wrong modal (eg transliterator)
        this.closeModal();
      }
    }
  },

  executeSelectedCommand() {
    this.executeCommand(this.commandPaletteSelected);
  },

  updateCommandPaletteQuery(query) {
    this.commandPaletteQuery = query;
    this.commandPaletteSelected = 0;
  },

  // Settings IO

  loadSettings() {
    const settingsStr = localStorage.getItem(CONFIG_KEY);
    if (settingsStr) {
      try {
        const settings = JSON.parse(settingsStr);
        this.textZoom = settings.textZoom || this.textZoom;
        // We can only get an accurate default zoom after the viewer is fully
        // initialized. See `init` for details.
        this.imageZoom = settings.imageZoom;
        this.layout = settings.layout || this.layout;
        this.viewMode = settings.viewMode || this.viewMode;

        this.fromScript = settings.fromScript || this.fromScript;
        this.toScript = settings.toScript || this.toScript;
        this.showAdvancedOptions = settings.showAdvancedOptions || this.showAdvancedOptions;

        // Normalize preferences (default to true if not set)
        this.normalizeReplaceColonVisarga = settings.normalizeReplaceColonVisarga !== undefined ? settings.normalizeReplaceColonVisarga : true;
        this.normalizeReplaceSAvagraha = settings.normalizeReplaceSAvagraha !== undefined ? settings.normalizeReplaceSAvagraha : true;
        this.normalizeReplaceDoublePipe = settings.normalizeReplaceDoublePipe !== undefined ? settings.normalizeReplaceDoublePipe : true;
      } catch (error) {
        // Old settings are invalid -- rewrite with valid values.
        this.saveSettings();
      }
    }
  },

  saveSettings() {
    const settings = {
      textZoom: this.textZoom,
      imageZoom: this.imageZoom,
      layout: this.layout,
      viewMode: this.viewMode,
      fromScript: this.fromScript,
      toScript: this.toScript,
      showAdvancedOptions: this.showAdvancedOptions,
      normalizeReplaceColonVisarga: this.normalizeReplaceColonVisarga,
      normalizeReplaceSAvagraha: this.normalizeReplaceSAvagraha,
      normalizeReplaceDoublePipe: this.normalizeReplaceDoublePipe,
    };
    localStorage.setItem(CONFIG_KEY, JSON.stringify(settings));
  },

  getLayoutClasses() {
    if (this.layout === ImageLayout.Left) {
      return ImageClasses.Left;
    } else if (this.layout === ImageLayout.Top) {
      return ImageClasses.Top;
    } else if (this.layout === ImageLayout.Bottom) {
      return ImageClasses.Bottom;
    }
    return ImageClasses.Right;
  },

  // Callbacks

  /** Displays a warning dialog if the user has unsaved changes and tries to navigate away. */
  onBeforeUnload(e) {
    if (this.hasUnsavedChanges) {
      // Keeps the dialog event.
      return true;
    }
    // Cancels the dialog event.
    return null;
  },

  // OCR controls

  async runOCR() {
    this.isRunningOCR = true;

    const { pathname } = window.location;
    const url = pathname.replace('/proofing/', '/api/ocr/');

    const content = await fetch(url)
      .then((response) => {
        if (response.ok) {
          return response.text();
        }
        return '(server error)';
      });
    Alpine.raw(this.editor).setText(content);
    $('#content').value = content;

    this.isRunningOCR = false;
  },

  // Currently disabled.
  async runLLMStructuring() {
    this.isRunningLLMStructuring = true;

    const { pathname } = window.location;
    const url = pathname.replace('/proofing/', '/api/llm-structuring/');
    const currentContent = Alpine.raw(this.editor).getText();

    const content = await fetch(url, {
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ content: currentContent }),
    })
      .then((response) => {
        if (response.ok) {
          return response.text();
        }
        return '(server error)';
      });
    Alpine.raw(this.editor).setText(content);
    $('#content').value = content;

    this.isRunningLLMStructuring = false;
  },

  async runStructuring() {
    this.isRunningStructuring = true;

    const { pathname } = window.location;
    const url = pathname.replace('/proofing/', '/api/structuring/');

    const currentContent = Alpine.raw(this.editor).getText();

    const content = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ content: currentContent }),
    })
      .then((response) => {
        if (response.ok) {
          return response.text();
        }
        return '(server error)';
      });
    Alpine.raw(this.editor).setText(content);
    $('#content').value = content;

    this.isRunningStructuring = false;
  },

  // Image zoom controls

  increaseImageZoom() {
    this.imageZoom *= 1.2;
    this.imageViewer.viewport.zoomTo(this.imageZoom);
    this.saveSettings();
  },
  decreaseImageZoom() {
    this.imageZoom *= 0.8;
    this.imageViewer.viewport.zoomTo(this.imageZoom);
    this.saveSettings();
  },
  resetImageZoom() {
    this.imageZoom = this.imageViewer.viewport.getHomeZoom();
    this.imageViewer.viewport.zoomTo(this.imageZoom);
    this.saveSettings();
  },

  // Text zoom controls

  increaseTextSize() {
    this.textZoom += 0.1;
    Alpine.raw(this.editor).setTextZoom(this.textZoom);
    this.saveSettings();
  },
  decreaseTextSize() {
    this.textZoom = Math.max(0.5, this.textZoom - 0.1);
    Alpine.raw(this.editor).setTextZoom(this.textZoom);
    this.saveSettings();
  },
  resetTextSize() {
    this.textZoom = 1;
    Alpine.raw(this.editor).setTextZoom(this.textZoom);
    this.saveSettings();
  },

  // Layout controls

  displayImageOnLeft() {
    this.layout = ImageLayout.Left;
    this.layoutClasses = this.getLayoutClasses();
    this.saveSettings();
  },
  displayImageOnRight() {
    this.layout = ImageLayout.Right;
    this.layoutClasses = this.getLayoutClasses();
    this.saveSettings();
  },
  displayImageOnTop() {
    this.layout = ImageLayout.Top;
    this.layoutClasses = this.getLayoutClasses();
    this.saveSettings();
  },
  displayImageOnBottom() {
    this.layout = ImageLayout.Bottom;
    this.layoutClasses = this.getLayoutClasses();
    this.saveSettings();
  },

  displayVisualView() {
    this.displayView(ViewType.Visual);
  },

  displayXMLView() {
    this.displayView(ViewType.XML);
  },

  displayView(viewMode) {
    // Already showing -- just return.
    if (this.viewMode === viewMode) return;

    // Store state before switching.
    const editorElement = $('#prosemirror-editor');
    const content = Alpine.raw(this.editor).getText();
    Alpine.raw(this.editor).destroy();

    if (viewMode === ViewType.Visual) {
      try {
        this.editor = new ProofingEditor(editorElement, content, () => {
          this.hasUnsavedChanges = true;
          $('#content').value = Alpine.raw(this.editor).getText();
        }, this.showAdvancedOptions, this.textZoom, (context) => {
          this.onActiveWordChange(context);
        });

      } catch (error) {
        this.xmlParseError = `Invalid XML: ${error.message}`;
        console.error('Failed to parse XML:', error);
        return;
      }
    } else if (viewMode === ViewType.XML) {
      this.editor = new XMLView(editorElement, content, () => {
        this.hasUnsavedChanges = true;
        $('#content').value = Alpine.raw(this.editor).getText();
      });
    }

    // Reset state + focus
    this.viewMode = viewMode;
    this.xmlParseError = null;
    this.saveSettings();
    Alpine.raw(this.editor).focus();
  },

  toggleAdvancedOptions() {
    this.showAdvancedOptions = !this.showAdvancedOptions;
    this.saveSettings();

    if (this.viewMode === ViewType.Visual && Alpine.raw(this.editor).setShowAdvancedOptions) {
      Alpine.raw(this.editor).setShowAdvancedOptions(this.showAdvancedOptions);
    }
  },

  changeSelectedText(callback) {
    const selection = Alpine.raw(this.editor).getSelection();
    const replacement = callback(selection.text);
    Alpine.raw(this.editor).replaceSelection(replacement);
  },

  toggleMark(markName) {
    Alpine.raw(this.editor).toggleMark(markName);
  },

  insertBlock() {
    Alpine.raw(this.editor).insertBlock();
  },

  deleteBlock() {
    Alpine.raw(this.editor).deleteActiveBlock();
  },

  moveBlockUp() {
    Alpine.raw(this.editor).moveBlockUp();
  },

  moveBlockDown() {
    Alpine.raw(this.editor).moveBlockDown();
  },

  undo() {
    Alpine.raw(this.editor).undo();
  },

  redo() {
    Alpine.raw(this.editor).redo();
  },

  replaceColonVisarga() {
    this.changeSelectedText((s) => s.replaceAll(':', 'ः'));
  },

  replaceSAvagraha() {
    this.changeSelectedText((s) => s.replaceAll('S', 'ऽ'));
  },

  openNormalizeModal() {
    this.activeModal = ModalType.Normalize;
  },


  applyNormalization() {
    this.changeSelectedText((text) => {
      let normalized = text;

      if (this.normalizeReplaceColonVisarga) {
        normalized = normalized.replaceAll(':', 'ः');
      }

      if (this.normalizeReplaceSAvagraha) {
        normalized = normalized.replaceAll('S', 'ऽ');
      }

      if (this.normalizeReplaceDoublePipe) {
        normalized = normalized.replaceAll('||', '॥');
      }

      return normalized;
    });

    this.saveSettings();
    this.closeModal();
  },

  openTransliterateModal() {
    this.activeModal = ModalType.Transliterate;
  },

  openAutoStructureModal() {
    this.activeModal = ModalType.AutoStructure;
  },


  async applyAutoStructure() {
    const content = Alpine.raw(this.editor).getText();
    const options = {
      stageDirections: this.autoStructureStageDirections,
      speakers: this.autoStructureSpeakers,
      chaya: this.autoStructureChaya,
    };

    try {
      this.isRunningStructuring = true;
      const response = await fetch(routes.proofingAutoStructure(), {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ content, options }),
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const data = await response.json();
      if (data.error) {
        this.xmlParseError = data.error;
      } else {
        Alpine.raw(this.editor).setText(data.content);
        this.closeModal();
      }
    } catch (error) {
      console.error('Auto-structure failed:', error);
      this.xmlParseError = 'Auto-structure failed: ' + error.message;
    } finally {
      this.isRunningStructuring = false;
    }
  },

  applyTransliteration() {
    this.changeSelectedText((s) => Sanscript.t(s, this.fromScript, this.toScript));
    this.saveSettings();
    this.closeModal();
  },

  transliterateSelectedText() {
    this.changeSelectedText((s) => Sanscript.t(s, this.fromScript, this.toScript));
    this.saveSettings();
  },

  copyCharacter(e) {
    const character = e.target.textContent;
    navigator.clipboard.writeText(character);
  },

  copyPageXML() {
    const content = Alpine.raw(this.editor).getText();
    navigator.clipboard.writeText(content);
  },

  async openHistoryModal() {
    this.activeModal = ModalType.History;
    this.historyLoading = true;
    this.historyRevisions = [];

    const { pathname } = window.location;
    const url = pathname.replace('/proofing/', '/api/proofing/') + '/history';

    try {
      const response = await fetch(url);
      if (response.ok) {
        const data = await response.json();
        this.historyRevisions = data.revisions || [];
      } else {
        console.error('Failed to fetch history:', response.status);
      }
    } catch (error) {
      console.error('Error fetching history:', error);
    } finally {
      this.historyLoading = false;
    }
  },


  getRevisionColorClass(status) {
    if (status === 'reviewed-0') {
      return 'bg-red-200 text-red-800';
    } else if (status === 'reviewed-1') {
      return 'bg-yellow-200 text-yellow-800';
    } else if (status === 'reviewed-2') {
      return 'bg-green-200 text-green-800';
    } else if (status === 'skip') {
      return 'bg-gray-200 text-gray-800';
    }
    return '';
  },

  openSubmitModal() {
    // Sync to text area.
    const currentContent = Alpine.raw(this.editor).getText();
    $('#content').value = currentContent;

    this.changesPreview = this.generateChangesPreview();

    this.modalSummary = $('input[name="summary"]')?.value || '';
    this.modalStatus = $('input[name="status"]')?.value || '';
    this.activeModal = ModalType.Submit;
  },


  generateChangesPreview() {
    const currentContent = Alpine.raw(this.editor).getText();

    // Trim and normalize whitespace for comparison
    const originalTrimmed = this.originalContent.trim();
    const currentTrimmed = currentContent.trim();

    if (originalTrimmed === currentTrimmed) {
      return '<span class="text-slate-500 italic">No changes made</span>';
    }

    // Simple diff: show both old and new content
    const originalLines = originalTrimmed.split('\n');
    const currentLines = currentTrimmed.split('\n');

    let diff = '';
    const maxLines = Math.max(originalLines.length, currentLines.length);

    // Show a simple comparison (first 15 changed lines)
    let changedCount = 0;
    let unchangedCount = 0;

    for (let i = 0; i < maxLines && changedCount < 15; i++) {
      const oldLine = originalLines[i] || '';
      const newLine = currentLines[i] || '';

      if (oldLine !== newLine) {
        changedCount++;
        unchangedCount = 0;

        if (oldLine && newLine) {
          diff += `<div class="text-red-700 bg-red-50 px-2 py-1 mb-0.5">- ${this.escapeHtml(oldLine)}</div>`;
          diff += `<div class="text-green-700 bg-green-50 px-2 py-1 mb-1">+ ${this.escapeHtml(newLine)}</div>`;
        } else if (oldLine) {
          diff += `<div class="text-red-700 bg-red-50 px-2 py-1 mb-1">- ${this.escapeHtml(oldLine)}</div>`;
        } else if (newLine) {
          diff += `<div class="text-green-700 bg-green-50 px-2 py-1 mb-1">+ ${this.escapeHtml(newLine)}</div>`;
        }
      } else {
        unchangedCount++;
        if (unchangedCount <= 2 && changedCount > 0) {
          diff += `<div class="text-slate-500 px-2 py-1 mb-0.5 text-xs">  ${this.escapeHtml(oldLine)}</div>`;
        }
      }
    }

    if (changedCount === 0) {
      return '<span class="text-slate-500 italic">Only whitespace changes detected</span>';
    }

    if (maxLines > 15 + changedCount) {
      diff += `<div class="text-slate-500 italic mt-2 text-xs">... and more changes (${maxLines} total lines)</div>`;
    }

    return diff || '<span class="text-slate-500 italic">Changes detected</span>';
  },

  escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  },

  submitFormFromModal() {
    const summaryInput = $('input[name="summary"]');
    const statusInput = $('input[name="status"]');

    if (summaryInput) summaryInput.value = this.modalSummary;
    if (statusInput) statusInput.value = this.modalStatus;

    this.closeModal();
    this.hasUnsavedChanges = false;

    const form = $('form');
    if (form) {
      form.submit();
    }
  },

  submitForm(e) {
    this.hasUnsavedChanges = false;
    e.target.submit();
  },

  // Bounding box highlighting
  // ----------------------------------------------

  onActiveWordChange(context) {
    if (!context || !this.boundingBoxLines.length || !this.imageViewer) {
      this.clearBoundingBoxHighlight();
      return;
    }

    const matchedBox = this.findBestMatchingBoundingBox(context);
    if (matchedBox) {
      this.highlightBoundingBox(matchedBox);
    } else {
      this.clearBoundingBoxHighlight();
    }
  },

  findBestMatchingBoundingBox(context) {
    const LINE_FUZZY_THRESHOLD = 0.7;
    const WORD_FUZZY_THRESHOLD = 0.7;

    const { word, lineText, wordIndex } = context;

    const normalizedWord = word.trim();
    const normalizedLine = lineText.trim();
    if (!normalizedWord || !normalizedLine) return null;

    let bestLine = null;
    let bestLineSimilarity = LINE_FUZZY_THRESHOLD;
    for (const line of this.boundingBoxLines) {
      const lineTextNormalized = line.text.toLowerCase().trim();
      if (lineTextNormalized === normalizedLine) {
        bestLine = line;
        break;
      }

      const similarity = similarityRatio(normalizedLine, lineTextNormalized);
      if (similarity > bestLineSimilarity) {
        bestLineSimilarity = similarity;
        bestLine = line;
      }
    }

    if (!bestLine) {
      return this.findBestMatchingBoundingBoxFallback(normalizedWord);
    }

    let bestWordBox = null;
    let bestWordSimilarity = WORD_FUZZY_THRESHOLD;

    for (const box of bestLine.boxes) {
      const boxText = box.text.toLowerCase();

      if (boxText === normalizedWord) {
        return box;
      }

      const similarity = similarityRatio(normalizedWord, boxText);
      if (similarity > bestWordSimilarity) {
        bestWordSimilarity = similarity;
        bestWordBox = box;
      }
    }

    return bestWordBox;
  },

  // Fallback to old algorithm when line matching fails
  findBestMatchingBoundingBoxFallback(normalizedWord) {
    for (const box of this.boundingBoxes) {
      if (box.text.toLowerCase() === normalizedWord) {
        return box;
      }
    }

    const FUZZY_THRESHOLD = 0.7;
    let bestMatch = null;
    let bestSimilarity = FUZZY_THRESHOLD;

    for (const box of this.boundingBoxes) {
      const boxText = box.text.toLowerCase();
      const similarity = similarityRatio(normalizedWord, boxText);

      if (similarity > bestSimilarity) {
        bestSimilarity = similarity;
        bestMatch = box;
      }
    }

    return bestMatch;
  },

  highlightBoundingBox(box) {
    this.clearBoundingBoxHighlight();

    if (!this.imageViewer || !this.imageViewer.world.getItemAt(0)) {
      return;
    }

    const tiledImage = this.imageViewer.world.getItemAt(0);
    const imageSize = tiledImage.getContentSize();

    // OpenSeadragon uses a coordinate system where the image width is normalized to 1.0
    // and all other dimensions (including y-axis) are scaled relative to the width.
    // This maintains the aspect ratio. So we divide ALL coordinates by image width.
    const x = box.x1 / imageSize.x;
    const y = box.y1 / imageSize.x;  // Note: dividing by width, not height
    const width = (box.x2 - box.x1) / imageSize.x;
    const height = (box.y2 - box.y1) / imageSize.x;  // Note: dividing by width, not height

    const overlayElement = document.createElement('div');
    overlayElement.className = 'ocr-bounding-box-highlight';
    overlayElement.style.border = '1px solid red';
    overlayElement.style.boxSizing = 'border-box';
    overlayElement.style.pointerEvents = 'none';

    this.imageViewer.addOverlay({
      element: overlayElement,
      location: new OpenSeadragon.Rect(x, y, width, height),
    });

    this.currentOverlay = overlayElement;
  },

  clearBoundingBoxHighlight() {
    if (this.currentOverlay) {
      try {
        this.imageViewer.removeOverlay(this.currentOverlay);
      } catch (e) {
        console.debug('Failed to remove overlay:', e);
      }
      this.currentOverlay = null;
    }
  },
});
