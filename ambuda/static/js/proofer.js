/* global Alpine, $, OpenSeadragon, Sanscript, IMAGE_URL */
/* Transcription and proofreading interface. */

import { $ } from './core.ts';
import ProofingEditor, { XMLView } from './prosemirror-editor.ts';

const CONFIG_KEY = 'proofing-editor';

const LAYOUT_IMAGE_LEFT = 'image-left';
const LAYOUT_IMAGE_RIGHT = 'image-right';
const LAYOUT_IMAGE_TOP = 'image-top';
const LAYOUT_IMAGE_BOTTOM = 'image-bottom';
const ALL_LAYOUTS = [LAYOUT_IMAGE_LEFT, LAYOUT_IMAGE_RIGHT, LAYOUT_IMAGE_TOP, LAYOUT_IMAGE_BOTTOM];

const CLASSES_IMAGE_LEFT = 'flex flex-row-reverse h-[90vh]';
const CLASSES_IMAGE_RIGHT = 'flex flex-row h-[90vh]';
const CLASSES_IMAGE_TOP = 'flex flex-col-reverse h-[90vh]';
const CLASSES_IMAGE_BOTTOM = 'flex flex-col h-[90vh]';

const VIEW_VISUAL = 'visual';
const VIEW_XML = 'xml';

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
  viewMode: VIEW_VISUAL,
  // [transliteration] the source script
  fromScript: 'hk',
  // [transliteration] the destination script
  toScript: 'devanagari',
  // If true, show advanced options (text, n, and merge_next)
  showAdvancedOptions: false,

  // Internal-only
  layoutClasses: CLASSES_IMAGE_RIGHT,
  isRunningOCR: false,
  isRunningLLMStructuring: false,
  isRunningStructuring: false,
  hasUnsavedChanges: false,
  xmlParseError: null,
  imageViewer: null,
  editor: null,
  commandPaletteOpen: false,
  commandPaletteQuery: '',
  commandPaletteSelected: 0,
  historyModalOpen: false,
  historyLoading: false,
  historyRevisions: [],

  init() {
    this.loadSettings();
    this.layoutClasses = this.getLayoutClasses();

    // Initialize editor (either ProofingEditor or XMLView based on viewMode)
    const editorElement = $('#prosemirror-editor');
    const initialContent = $('#content').value || '';

    // NOTE: always use Alpine.raw() to access the editor because Alpine reactivity/proxies breaks
    // the underlying data model and causes bizarre errors, e.g.:
    //
    // https://discuss.prosemirror.net/t/getting-rangeerror-applying-a-mismatched-transaction-even-with-trivial-code/4948/3
    if (this.viewMode === VIEW_XML) {
      this.editor = new XMLView(editorElement, initialContent, () => {
        this.hasUnsavedChanges = true;
        $('#content').value = Alpine.raw(this.editor).getText();
      });
    } else {
      this.editor = new ProofingEditor(editorElement, initialContent, () => {
        this.hasUnsavedChanges = true;
        $('#content').value = Alpine.raw(this.editor).getText();
      }, this.showAdvancedOptions);
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
    return [
      { label: 'Edit > Undo', action: () => this.undo() },
      { label: 'Edit > Redo', action: () => this.redo() },
      { label: 'Edit > Insert block', action: () => this.insertBlock() },
      { label: 'Edit > Delete active block', action: () => this.deleteBlock() },
      { label: 'Edit > Mark as error', action: () => this.markAsError() },
      { label: 'Edit > Mark as fix', action: () => this.markAsFix() },
      { label: 'Edit > Mark as unclear', action: () => this.markAsUnclear() },
      { label: 'Edit > Mark as footnote number', action: () => this.markAsFootnoteNumber() },
      { label: 'View > Show image on left', action: () => this.displayImageOnLeft() },
      { label: 'View > Show image on right', action: () => this.displayImageOnRight() },
      { label: 'View > Show image on top', action: () => this.displayImageOnTop() },
      { label: 'View > Show image on bottom', action: () => this.displayImageOnBottom() },
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
    this.commandPaletteOpen = true;
    this.commandPaletteQuery = '';
    this.commandPaletteSelected = 0;
    this.$nextTick(() => {
      const input = document.querySelector('#command-palette-input');
      if (input) input.focus();
    });
  },

  closeCommandPalette() {
    this.commandPaletteOpen = false;
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
      this.closeCommandPalette();
    }
  },

  executeCommand(index) {
    const filtered = this.getFilteredCommands();
    if (filtered[index]) {
      filtered[index].action();
      this.closeCommandPalette();
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
    };
    localStorage.setItem(CONFIG_KEY, JSON.stringify(settings));
  },

  getLayoutClasses() {
    if (this.layout === LAYOUT_IMAGE_LEFT) {
      return CLASSES_IMAGE_LEFT;
    } else if (this.layout === LAYOUT_IMAGE_TOP) {
      return CLASSES_IMAGE_TOP;
    } else if (this.layout === LAYOUT_IMAGE_BOTTOM) {
      return CLASSES_IMAGE_BOTTOM;
    }
    return CLASSES_IMAGE_RIGHT;
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
    this.textZoom += 0.2;
    this.saveSettings();
  },
  decreaseTextSize() {
    this.textZoom = Math.max(0, this.textZoom - 0.2);
    this.saveSettings();
  },

  // Layout controls

  displayImageOnLeft() {
    this.layout = LAYOUT_IMAGE_LEFT;
    this.layoutClasses = this.getLayoutClasses();
    this.saveSettings();
  },
  displayImageOnRight() {
    this.layout = LAYOUT_IMAGE_RIGHT;
    this.layoutClasses = this.getLayoutClasses();
    this.saveSettings();
  },
  displayImageOnTop() {
    this.layout = LAYOUT_IMAGE_TOP;
    this.layoutClasses = this.getLayoutClasses();
    this.saveSettings();
  },
  displayImageOnBottom() {
    this.layout = LAYOUT_IMAGE_BOTTOM;
    this.layoutClasses = this.getLayoutClasses();
    this.saveSettings();
  },

  displayVisualView() {
    this.displayView(VIEW_VISUAL);
  },

  displayXMLView() {
    this.displayView(VIEW_XML);
  },

  displayView(viewMode) {
    // Already showing -- just return.
    if (this.viewMode === viewMode) return;

    // Store state before switching.
    const editorElement = $('#prosemirror-editor');
    const content = Alpine.raw(this.editor).getText();
    Alpine.raw(this.editor).destroy();

    if (viewMode === VIEW_VISUAL) {
      try {
        this.editor = new ProofingEditor(editorElement, content, () => {
          this.hasUnsavedChanges = true;
          $('#content').value = Alpine.raw(this.editor).getText();
        }, this.showAdvancedOptions);

      } catch (error) {
        this.xmlParseError = `Invalid XML: ${error.message}`;
        console.error('Failed to parse XML:', error);
        return;
      }
    } else if (viewMode === VIEW_XML) {
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

    if (this.viewMode === VIEW_VISUAL && Alpine.raw(this.editor).setShowAdvancedOptions) {
      Alpine.raw(this.editor).setShowAdvancedOptions(this.showAdvancedOptions);
    }
  },

  changeSelectedText(callback) {
    const selection = Alpine.raw(this.editor).getSelection();
    const replacement = callback(selection.text);
    Alpine.raw(this.editor).replaceSelection(replacement);
  },

  markAsError() {
    Alpine.raw(this.editor).toggleMark('error');
  },

  markAsFix() {
    Alpine.raw(this.editor).toggleMark('fix');
  },

  markAsUnclear() {
    Alpine.raw(this.editor).toggleMark('flag');
  },

  markAsFootnoteNumber() {
    Alpine.raw(this.editor).toggleMark('ref');
  },

  insertBlock() {
    Alpine.raw(this.editor).insertBlock();
  },

  deleteBlock() {
    Alpine.raw(this.editor).deleteActiveBlock();
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

  transliterateSelectedText() {
    this.changeSelectedText((s) => Sanscript.t(s, this.fromScript, this.toScript));
    this.saveSettings();
  },

  copyCharacter(e) {
    const character = e.target.textContent;
    navigator.clipboard.writeText(character);
  },

  async openHistoryModal() {
    this.historyModalOpen = true;
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

  closeHistoryModal() {
    this.historyModalOpen = false;
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

  submitForm(e) {
    this.hasUnsavedChanges = false;
    e.target.submit();
  },
});
