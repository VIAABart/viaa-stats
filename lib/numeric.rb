class Numeric
  def to(unit, places=2)
    units = { :b => 1,
              :kb => 1024**1,
              :mb => 1024**2,
              :gb => 1024**3,
              :tb => 1024**4,
              :pb => 1024**5,
              :eb => 1024**6}
    unitval = units[unit.to_s.downcase.to_sym]
    "#{sprintf("%.#{places}f", self.to_f / unitval)}" # "#{unit.to_s.upcase}"
  end # to
end