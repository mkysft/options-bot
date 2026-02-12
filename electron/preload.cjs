const { contextBridge } = require("electron");

contextBridge.exposeInMainWorld("optionsBotDesktop", {
  isElectron: true,
  platform: process.platform
});

