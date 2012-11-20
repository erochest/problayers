guard 'shell' do
  watch(/(.*).hs/) { |m| `cabal-dev build && ./dist/build/problayers/problayers` }
end
